package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.{CassandraException, InvalidPayloadException, InvalidUserIdException, MissingPayloadException}
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.karmapoints.v2.utils.TransactionIdGenerator
import org.sunbird.job.util.JSONUtil

import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

/** Fields extracted once validation passes, so `doHandle` never re-parses `event.data`.
 * `originalTransactionId`/`originalCreatedAt` identify the DEBIT this reaward reverses - distinct
 * from `plan.transactionId`/`plan.createdAt` below, which are this reaward's OWN transaction. */
private[v2] case class CoinsReawardRequest(userId: String, operation: String, actionType: String,
                                           coinsToReaward: Long, contextType: String, contextId: String,
                                           originalTransactionId: String, originalCreatedAt: Long)

/** Business calculation result, reused to build the frozen plan below. No monthly-cap fields, same
 * as [[CoinsRedemptionHandler.CoinsRedemptionCalculation]] - a reaward, like a redemption, never
 * touches `user_karma_coin_monthly_summary`. */
private[v2] case class CoinsReawardCalculation(totalEarned: Int, totalRedeemed: Int, targetTotalRedeemed: Int)

/** The frozen plan for a reaward - same purpose/shape as [[CoinsRedemptionHandler.RedemptionPlan]]:
 * computed once in [[CoinsReawardHandler.createAndPersistReawardPlan]] and persisted into the
 * lookup row's `addinfo` before any wallet/transaction write. `transactionId`/`createdAt` here are
 * this reaward's OWN identity - always freshly generated, never the original DEBIT's.
 * `coinsToReaward` is frozen here too (not read from `request` in [[CoinsReawardHandler.applyReawardPlan]])
 * so a PROCESSING replay always applies the exact amount `targetTotalRedeemed` was computed from,
 * even if it somehow differed from the replay's own request. */
private[v2] case class ReawardPlan(transactionId: String, createdAt: Long, targetTotalEarned: Int,
                                   targetTotalRedeemed: Int, coinsToReaward: Long)

/** Outcome of [[CoinsReawardHandler.claimOrResumeReaward]]. Named distinctly from
 * [[CoinsRedemptionHandler]]'s `RedemptionClaimOutcome` family (same package, would otherwise collide). */
private[v2] sealed trait ReawardClaimOutcome

private[v2] case object ReawardAlreadySucceeded extends ReawardClaimOutcome

private[v2] case class ReawardProceed(creditDate: Long) extends ReawardClaimOutcome

private[v2] case class ReawardResumeWithPlan(plan: ReawardPlan) extends ReawardClaimOutcome

/**
 * Handles COINS_REAWARD (CREDIT) events to return previously redeemed
 * Karma Coins to the user's wallet when a redemption needs to be reversed.
 * Validates the original redemption and calculates the coins to be reawarded.
 * Updates the wallet and records the reaward transaction.
 */
class CoinsReawardHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private val logger = LoggerFactory.getLogger(classOf[CoinsReawardHandler])

  /** Test-only seam: exposes whether/what this handler was invoked with, without adding business logic. */
  private[v2] var lastHandledEvent: Option[UnifiedEvent] = None

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    // Reset before this event's own claim attempt, so a prior event's key can never leak into
    // this event's exception-cleanup decision (see EventHandler.lastClaimedDedupKey's doc).
    lastClaimedDedupKey = None
    val request = validateEvent(event)
    val requestKey = userKarmaCoinKey(request)
    val redisKey = redisDedupKey(request)
    val dedupEnabled = config.coinsReawardDedupEnabled

    if (dedupEnabled && !redisUtil.claimKarmaCoinDedup(redisKey, event.getJson())) {
      // Redis has already seen this exact request - skip immediately, Cassandra is not consulted.
      logger.info(s"Duplicate COINS_REAWARD per Redis first-level dedup, userKarmaCoinKey=$requestKey, skipping")
      metrics.incCounter(config.skippedEventCount)
      return
    }
    // We claimed it - record the exact (namespaced) key that was actually used, NOT
    // userKarmaCoinKey(request). Release-on-exception is now owned centrally by
    // KarmaPointsProcessorFnV2.processElement's finally (via this field) rather than here - the
    // former handler-level try/catch that used to call releaseKarmaCoinRequestClaim on any
    // exception before rethrowing has been removed as redundant with that central cleanup.
    if (dedupEnabled) lastClaimedDedupKey = Some(redisKey)

    claimOrResumeReaward(request) match {
      case ReawardAlreadySucceeded =>
        // Already logged/metered inside claimOrResume.

      case ReawardProceed(creditDate) =>
        val calculation = try {
          calculateReaward(request)
        } catch {
          case ex: InvalidPayloadException =>
            // Business-rule rejection - mark the lookup FAILED before letting the existing
            // DataQualityException/failed-topic path handle it.
            insertFailedReawardTransaction(request)
            updateLookupStatus(request, creditDate, config.STATUS_FAILED,
              config.ADDINFO_ERROR_CODE -> config.ERROR_CODE_INVALID_REAWARD,
              config.ADDINFO_ERROR_MESSAGE -> ex.message)
            throw ex
        }
        val plan = createAndPersistReawardPlan(request, calculation, creditDate)
        applyReawardPlan(request, event, plan)
        lastHandledEvent = Some(event)
        logger.info(s"COINS_REAWARD completed successfully: userId=${request.userId}, contextId=${request.contextId}, " +
          s"coinsToReaward=${request.coinsToReaward}, originalTransactionId=${request.originalTransactionId}")

      case ReawardResumeWithPlan(plan) =>
        // Plan was already frozen by a prior crashed attempt - re-apply it as-is, never
        // recompute; safe regardless of how far that prior attempt got.
        applyReawardPlan(request, event, plan)
        lastHandledEvent = Some(event)
        logger.info(s"COINS_REAWARD resumed from persisted plan: userId=${request.userId}, " +
          s"contextId=${request.contextId}, transactionId=${plan.transactionId}")
    }
  }

  /** Full envelope validation for COINS_REAWARD, in order: userId -> operation -> actionType ->
   * coinsToReaward -> contextType -> contextId -> transactionId -> createdAt. Throws on the first
   * failing check. Everything checked here is the envelope only - the original-DEBIT validations
   * (existence, type, amount match, etc.) need a Cassandra read and live in [[calculateReaward]]. */
  private[v2] def validateEvent(event: UnifiedEvent): CoinsReawardRequest = {
    val userId = event.dataString("userId")
    if (StringUtils.isEmpty(userId)) {
      throw InvalidUserIdException(
        s"data.userId is missing/empty for eventType=${event.eventType}, mid=${event.mid()}"
      )
    }
    val operation = event.dataString("operation")
    if (!config.OPERATION_CREDIT.equals(operation)) {
      throw InvalidPayloadException(
        s"data.operation must be '${config.OPERATION_CREDIT}' for COINS_REAWARD event, " +
          s"got '$operation', userId=$userId"
      )
    }
    val actionType = event.dataString("actionType")
    if (!config.EVENT_TYPE_COINS_REAWARD.equals(actionType)) {
      throw InvalidPayloadException(
        s"data.actionType must be '${config.EVENT_TYPE_COINS_REAWARD}' for COINS_REAWARD event, " +
          s"got '$actionType', userId=$userId"
      )
    }
    val coinsToReaward = event.dataLong("coinsToReaward", 0L)
    if (coinsToReaward <= 0) {
      throw InvalidPayloadException(
        s"data.coinsToReaward must be > 0 for COINS_REAWARD event, " +
          s"got '$coinsToReaward', userId=$userId"
      )
    }
    val contextType = event.dataString("contextType")
    if (StringUtils.isEmpty(contextType)) {
      throw MissingPayloadException(
        s"data.contextType is required for COINS_REAWARD event, userId=$userId"
      )
    }
    val contextId = event.dataString("contextId")
    if (StringUtils.isEmpty(contextId)) {
      throw MissingPayloadException(
        s"data.contextId is required for COINS_REAWARD event, userId=$userId"
      )
    }
    val originalTransactionId = event.dataString("transactionId")
    if (StringUtils.isEmpty(originalTransactionId)) {
      throw MissingPayloadException(
        s"data.transactionId is required for COINS_REAWARD event, userId=$userId"
      )
    }
    val originalCreatedAt = event.dataLong("createdAt", 0L)
    if (originalCreatedAt <= 0) {
      throw MissingPayloadException(
        s"data.createdAt is required for COINS_REAWARD event, userId=$userId"
      )
    }
    CoinsReawardRequest(userId, operation, actionType, coinsToReaward, contextType, contextId,
      originalTransactionId, originalCreatedAt)
  }

  /** `userId|contextType|contextId` - the Cassandra lookup's `user_karma_coin_key`, SAME value as
   * the original COINS_REDEMPTION's (a reaward always targets the same enrollment context as the
   * redemption it reverses). Safe to reuse unprefixed here because `user_karma_coin_lookup`'s
   * primary key is `(user_karma_coin_key, operation_type)` - this row's `operation_type=CREDIT`
   * (from `data.operation`) already keeps it separate from the COINS_REDEMPTION row's
   * `operation_type=DEBIT` for the same key. */
  private[v2] def userKarmaCoinKey(request: CoinsReawardRequest): String =
    request.userId + config.PIPE + request.contextType + config.PIPE + request.contextId

  /** Redis's `claimKarmaCoinDedup` has no operation-type dimension the way Cassandra's lookup table
   * does - it just SET-NX's whatever key string it's given. Since [[userKarmaCoinKey]] is
   * IDENTICAL to the original redemption's dedup key for the same contextId, reusing it unprefixed
   * here would collide with COINS_REDEMPTION's own Redis claim for that same request. Namespacing
   * with the operation-type literal keeps the two first-level dedup claims independent. */
  private[v2] def redisDedupKey(request: CoinsReawardRequest): String =
    config.EVENT_TYPE_COINS_REAWARD + config.PIPE + userKarmaCoinKey(request)

  /**
   * Resolves the current reaward state using the Cassandra lookup record - identical shape to
   * [[CoinsRedemptionHandler.claimOrResumeRedemption]]. New requests are claimed using Cassandra
   * LWT; existing FAILED or PROCESSING requests are handled according to their persisted state,
   * while completed requests are skipped. Cassandra failures are propagated to the existing Flink
   * retry/restart mechanism.
   */
  private[v2] def claimOrResumeReaward(request: CoinsReawardRequest)(implicit metrics: Metrics): ReawardClaimOutcome = {
    val requestKey = userKarmaCoinKey(request)
    val freshCreditDate = System.currentTimeMillis()
    val freshAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)

    val claimed = cassandraUtil.claimKarmaCoinLookup(requestKey, request.operation, freshCreditDate, freshAddInfo)
    if (claimed) {
      return ReawardProceed(freshCreditDate)
    }

    // Contention: a row already exists. Read it once to find out why.
    val existing = cassandraUtil.fetchKarmaCoinLookup(requestKey, request.operation)
    if (existing == null || existing.isEmpty) {
      logger.error(
        s"COINS_REAWARD lookup row not found after claim failed, " +
          s"requestKey=$requestKey. The row may have disappeared between claim and read."
      )
      throw CassandraException(s"user_karma_coin_lookup row for key=$requestKey disappeared between claim and read")
    }
    val existingAddInfo = existing.get(0).getString(config.ADD_INFO)
    val existingCreditDate = existing.get(0).getLong(config.DB_COLUMN_CREDIT_DATE)
    val statusMap = if (StringUtils.isEmpty(existingAddInfo)) new java.util.HashMap[String, Any]()
    else JSONUtil.deserialize[java.util.Map[String, Any]](existingAddInfo)
    val status = statusMap.getOrDefault(config.STATUS, config.EMPTY).asInstanceOf[String]

    status match {
      case s if config.STATUS_SUCCESS.equals(s) =>
        logger.info(s"Duplicate COINS_REAWARD - SUCCESS already recorded for userKarmaCoinKey=$requestKey, skipping")
        metrics.incCounter(config.skippedEventCount)
        ReawardAlreadySucceeded

      case s if config.STATUS_FAILED.equals(s) =>
        val newAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)
        val transitioned = cassandraUtil.transitionKarmaCoinLookup(requestKey, request.operation,
          existingAddInfo, newAddInfo, freshCreditDate)
        if (!transitioned) {
          throw CassandraException(s"Could not CAS FAILED->PROCESSING for user_karma_coin_lookup key=$requestKey (contention)")
        }
        ReawardProceed(freshCreditDate)

      case s if config.STATUS_PROCESSING.equals(s) =>
        parseReawardPlan(statusMap) match {
          case Some(plan) => ReawardResumeWithPlan(plan)
          case None =>
            ReawardProceed(existingCreditDate)
        }

      case other =>
        throw CassandraException(s"Unrecognized user_karma_coin_lookup status='$other' for key=$requestKey")
    }
  }

  /** Upserts the lookup row to `status` (+ extra addinfo fields) - replaces the whole addinfo, same
   * as [[CoinsRedemptionHandler.updateLookupStatus]] (kept as a separate local copy - the flows'
   * addinfo shapes diverge from here on). */
  private[v2] def updateLookupStatus(request: CoinsReawardRequest, creditDate: Long, status: String,
                                     extraFields: (String, Any)*)(implicit metrics: Metrics): Unit = {
    val addInfo = cassandraUtil.buildAddInfo(null, (config.STATUS -> status) +: extraFields: _*)
    cassandraUtil.updateKarmaCoinLookup(userKarmaCoinKey(request), request.operation, creditDate, addInfo)
  }

  /** `(total_earned, total_redeemed)` for the user's wallet - same table/shape as
   * [[CoinsRedemptionHandler.readWallet]]. No wallet row yet -> (0, 0) (in practice unreachable
   * here, since a reaward always requires a pre-existing DEBIT, which itself requires a wallet row -
   * kept for symmetry with the rest of this handler's Cassandra reads). */
  private[v2] def readWallet(userId: String): (Int, Int) = {
    val rows = cassandraUtil.fetchKarmaCoinWallet(userId)
    if (rows != null && rows.size() > 0) (rows.get(0).getInt(config.TOTAL_EARNED), rows.get(0).getInt(config.TOTAL_REDEEMED))
    else (0, 0)
  }

  /**
   * Business calculation, happy-path only (reads + validates the original DEBIT, then computes the
   * decremented wallet target - no writes): fetch the original transaction by its own primary key
   * (`userId`, `createdAt`, `transactionId`) -> validate it's a DEBIT/POINTS_REDEMPTION for this
   * same contextType/contextId with the exact `coinsToReaward` amount -> read wallet -> compute the
   * decrement, guarding against a negative `total_redeemed`.
   *
   * @throws InvalidPayloadException on any mismatch (missing/wrong-type/wrong-context/wrong-amount
   *                                 original transaction, or a resulting negative balance) - a
   *                                 business rejection, not an infra failure. The caller marks the
   *                                 lookup FAILED before letting this propagate.
   */
  private[v2] def calculateReaward(request: CoinsReawardRequest): CoinsReawardCalculation = {
    val originalRows = cassandraUtil.fetchKarmaCoinTransaction(request.userId, request.originalCreatedAt, request.originalTransactionId)
    if (originalRows == null || originalRows.isEmpty) {
      throw InvalidPayloadException(
        s"No original transaction found for userId=${request.userId}, " +
          s"transactionId=${request.originalTransactionId}, createdAt=${request.originalCreatedAt} - " +
          s"cannot process COINS_REAWARD")
    }
    val original = originalRows.get(0)

    val originalType = original.getString(config.TYPE)
    if (!config.OPERATION_DEBIT.equals(originalType)) {
      throw InvalidPayloadException(
        s"Original transactionId=${request.originalTransactionId} has type='$originalType', " +
          s"expected '${config.OPERATION_DEBIT}' for userId=${request.userId}")
    }
    val originalActionType = original.getString(config.ACTION_TYPE)
    if (!config.ACTION_TYPE_POINTS_REDEMPTION.equals(originalActionType)) {
      throw InvalidPayloadException(
        s"Original transactionId=${request.originalTransactionId} has actionType='$originalActionType', " +
          s"expected '${config.ACTION_TYPE_POINTS_REDEMPTION}' for userId=${request.userId}")
    }
    val originalContextType = original.getString(config.CONTEXT_TYPE)
    val originalContextId = original.getString(config.CONTEXT_ID)
    if (!request.contextType.equals(originalContextType) || !request.contextId.equals(originalContextId)) {
      throw InvalidPayloadException(
        s"COINS_REAWARD contextType/contextId ('${request.contextType}'/'${request.contextId}') does not match " +
          s"original transactionId=${request.originalTransactionId}'s ('$originalContextType'/'$originalContextId') " +
          s"for userId=${request.userId}")
    }
    val originalAmount = original.getLong(config.AMOUNT)
    if (originalAmount != request.coinsToReaward) {
      throw InvalidPayloadException(
        s"data.coinsToReaward (${request.coinsToReaward}) does not match original transactionId=" +
          s"${request.originalTransactionId}'s DEBIT amount ($originalAmount) for userId=${request.userId}")
    }

    val (totalEarned, totalRedeemed) = readWallet(request.userId)
    val targetTotalRedeemed = totalRedeemed - request.coinsToReaward.toInt
    if (targetTotalRedeemed < 0) {
      throw InvalidPayloadException(
        s"COINS_REAWARD of ${request.coinsToReaward} would make total_redeemed negative " +
          s"(current=$totalRedeemed) for userId=${request.userId}, transactionId=${request.originalTransactionId}")
    }
    CoinsReawardCalculation(totalEarned, totalRedeemed, targetTotalRedeemed)
  }

  /** Parses a persisted [[ReawardPlan]] from a PROCESSING row's addinfo, if one was already frozen
   * by a prior attempt - presence of `transactionId` means a plan exists. */
  private def parseReawardPlan(statusMap: java.util.Map[String, Any]): Option[ReawardPlan] = {
    if (!statusMap.containsKey(config.ADDINFO_TRANSACTION_ID)) {
      None
    } else {
      Some(ReawardPlan(
        transactionId = statusMap.get(config.ADDINFO_TRANSACTION_ID).toString,
        createdAt = statusMap.get(config.ADDINFO_CREATED_AT).asInstanceOf[Number].longValue(),
        targetTotalEarned = statusMap.get(config.ADDINFO_TARGET_TOTAL_EARNED).asInstanceOf[Number].intValue(),
        targetTotalRedeemed = statusMap.get(config.ADDINFO_TARGET_TOTAL_REDEEMED).asInstanceOf[Number].intValue(),
        coinsToReaward = statusMap.get(config.ADDINFO_COINS_REAWARDED).asInstanceOf[Number].longValue()
      ))
    }
  }

  /** Freezes the plan: persists the target wallet values and a NEW transaction id ONCE, into the
   * still-PROCESSING lookup row's addinfo, before any wallet/transaction write is attempted -
   * `transactionId`/`createdAt` here are always freshly generated for THIS reaward event, never
   * reused from the original DEBIT being reversed (see [[CoinsReawardRequest]]'s doc).
   * `targetTotalEarned` is carried through unchanged (a reaward never mutates it), so
   * [[applyReawardPlan]] can upsert the wallet from the plan alone. */
  private[v2] def createAndPersistReawardPlan(request: CoinsReawardRequest, calculation: CoinsReawardCalculation,
                                              creditDate: Long)(implicit metrics: Metrics): ReawardPlan = {
    val plan = ReawardPlan(
      transactionId = TransactionIdGenerator.generate(config),
      createdAt = creditDate,
      targetTotalEarned = calculation.totalEarned,
      targetTotalRedeemed = calculation.targetTotalRedeemed,
      coinsToReaward = request.coinsToReaward
    )
    updateLookupStatus(request, creditDate, config.STATUS_PROCESSING,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId,
      config.ADDINFO_CREATED_AT -> plan.createdAt,
      config.ADDINFO_TARGET_TOTAL_EARNED -> plan.targetTotalEarned,
      config.ADDINFO_TARGET_TOTAL_REDEEMED -> plan.targetTotalRedeemed,
      config.ADDINFO_COINS_REAWARDED -> plan.coinsToReaward)
    plan
  }

  /** Applies a frozen plan: wallet -> CREDIT (reaward) transaction -> lookup SUCCESS -> Redis
   * refresh, in that order (Cassandra first, Redis best-effort last) - same order/idempotency shape
   * as [[CoinsRedemptionHandler.applyRedemptionPlan]]. No `user_karma_coin_monthly_summary` write -
   * a reaward never touches it. The original DEBIT row is never modified or deleted; the new
   * transaction's addinfo carries `originalTransactionId`/`originalCreatedAt` so the reversal can
   * always be traced back to it. */
  private[v2] def applyReawardPlan(request: CoinsReawardRequest, event: UnifiedEvent, plan: ReawardPlan)
                                  (implicit metrics: Metrics): Unit = {
    logger.info(
      s"Applying COINS_REAWARD plan, " +
        s"userId=${request.userId}, contextId=${request.contextId}, " +
        s"transactionId=${plan.transactionId}, " +
        s"targetTotalEarned=${plan.targetTotalEarned}, " +
        s"targetTotalRedeemed=${plan.targetTotalRedeemed}"
    )
    cassandraUtil.updateKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed)

    val balanceAfter = plan.targetTotalEarned - plan.targetTotalRedeemed
    val transactionAddInfo = cassandraUtil.buildAddInfo(null,
      config.ADDINFO_COURSE_NAME -> event.dataString("courseName"),
      config.ADDINFO_PROVIDER_NAME -> event.dataString("providerName"),
      config.ADDINFO_INFO -> event.dataString("info"),
      config.ADDINFO_ORIGINAL_TRANSACTION_ID -> request.originalTransactionId,
      config.ADDINFO_ORIGINAL_CREATED_AT -> request.originalCreatedAt)
    cassandraUtil.insertKarmaCoinTransaction(request.userId, plan.createdAt, plan.transactionId, config.OPERATION_CREDIT,
      plan.coinsToReaward, balanceAfter, config.EVENT_TYPE_COINS_REAWARD,
      request.contextType, request.contextId, transactionAddInfo)
    logger.info(
      s"COINS_REAWARD transaction persisted, " +
        s"userId=${request.userId}, transactionId=${plan.transactionId}, " +
        s"amount=${plan.coinsToReaward}, balanceAfter=$balanceAfter, " +
        s"originalTransactionId=${request.originalTransactionId}"
    )

    updateLookupStatus(request, plan.createdAt, config.STATUS_SUCCESS,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId, config.ADDINFO_COINS_REAWARDED -> plan.coinsToReaward)
    logger.info(
      s"COINS_REAWARD lookup marked SUCCESS, " +
        s"userId=${request.userId}, contextId=${request.contextId}, " +
        s"transactionId=${plan.transactionId}"
    )

    // A reaward never writes user_karma_coin_monthly_summary, so reading the current month's figure
    // here (rather than hardcoding 0) avoids clobbering a legitimately-cached CREDIT value - same
    // reasoning as CoinsRedemptionHandler.applyRedemptionPlan.
    val (yearMonth, convertedThisMonth) = currentPointsConvertedThisMonth(request.userId)
    redisUtil.setKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed, yearMonth, convertedThisMonth)
  }

  /** Current calendar month's `points_converted`, reused as-is from
   * [[CassandraUtil.fetchKarmaCoinMonthlySummary]] - see [[applyReawardPlan]] for why. No row yet -> 0. */
  private[v2] def currentPointsConvertedThisMonth(userId: String): (String, Int) = {
    // Explicit Asia/Kolkata - do not rely on the JVM/TaskManager default timezone (H2 fix).
    val yearMonth = LocalDate.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern(config.YYYY_DASH_MM))
    val rows = cassandraUtil.fetchKarmaCoinMonthlySummary(userId, yearMonth)
    val converted = if (rows != null && rows.size() > 0) rows.get(0).getInt(config.POINTS_CONVERTED) else 0
    (yearMonth, converted)
  }

  /** Records a business-failure FAILED transaction with a NEW transactionId/createdAt - never the
   * frozen plan's identity - and the wallet's current (unmodified) balance as balance_after. */
  private[v2] def insertFailedReawardTransaction(request: CoinsReawardRequest)(implicit metrics: Metrics): Unit = {
    val (totalEarned, totalRedeemed) = readWallet(request.userId)
    val failedAddInfo = cassandraUtil.buildAddInfo(null,
      config.STATUS -> config.STATUS_FAILED,
      config.ADDINFO_ORIGINAL_TRANSACTION_ID -> request.originalTransactionId,
      config.ADDINFO_ORIGINAL_CREATED_AT -> request.originalCreatedAt)
    cassandraUtil.insertKarmaCoinTransaction(request.userId, System.currentTimeMillis(), TransactionIdGenerator.generate(config),
      config.OPERATION_CREDIT, request.coinsToReaward, totalEarned - totalRedeemed,
      request.actionType, request.contextType, request.contextId, failedAddInfo)
  }
}
