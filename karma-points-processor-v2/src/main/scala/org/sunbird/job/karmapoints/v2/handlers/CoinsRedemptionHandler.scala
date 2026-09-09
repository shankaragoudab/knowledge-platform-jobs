package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.{CassandraException, InvalidPayloadException, InvalidUserIdException, MissingPayloadException}
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.util.JSONUtil

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

/** Fields extracted once validation passes, so `doHandle` never re-parses `event.data`. */
private[v2] case class CoinsRedemptionRequest(userId: String, operation: String, actionType: String,
                                              pointsToConvert: Long, contextType: String, contextId: String)

/** Business calculation result, reused to build the frozen plan below. No monthly-cap fields
 * (unlike [[PointsConversionHandler.PointsConversionCalculation]]) - DEBIT has no monthly cap and
 * never touches `user_karma_coin_monthly_summary`. */
private[v2] case class CoinsRedemptionCalculation(totalEarned: Int, totalRedeemed: Int, targetTotalRedeemed: Int, targetBalance: Int)

/** The frozen plan for a DEBIT - computed once in [[CoinsRedemptionHandler.createAndPersistRedemptionPlan]] and
 * persisted into the lookup row's `addinfo` before any wallet/transaction write, same idempotency
 * purpose as [[PointsConversionHandler]]'s `ConversionPlan`. No `targetYearMonth`/
 * `targetPointsConverted` - those are CREDIT's monthly-cap bookkeeping, with no DEBIT equivalent. */
private[v2] case class RedemptionPlan(transactionId: String, createdAt: Long, targetTotalEarned: Int, targetTotalRedeemed: Int)

/** Outcome of [[CoinsRedemptionHandler.claimOrResumeRedemption]]. Named distinctly from
 * [[PointsConversionHandler]]'s `ClaimOutcome` family (same package, would otherwise collide). */
private[v2] sealed trait RedemptionClaimOutcome

private[v2] case object RedemptionAlreadySucceeded extends RedemptionClaimOutcome

private[v2] case class RedemptionProceed(creditDate: Long) extends RedemptionClaimOutcome

private[v2] case class RedemptionResumeWithPlan(plan: RedemptionPlan) extends RedemptionClaimOutcome

/**
 * Handles COINS_REDEMPTION (DEBIT) events - a user spending Karma Coins (e.g. external course
 * enrollment). Payload: `data.userId`, `data.operation` ("DEBIT"), `data.actionType`
 * ("POINTS_REDEMPTION"), `data.pointsToConvert`, `data.contextType`, `data.contextId`. Unlike
 * POINTS_CONVERSION's `contextType`, this one is an external-context discriminator (e.g.
 * "EXT_COURSE_ENROLLMENT"), not a fixed literal - only presence is validated. `courseName`/
 * `providerName` are carried through to the transaction addinfo but are not mandatory.
 *
 * Flow: validate -> Redis first-level dedup (when `coinsRedemptionDedupEnabled`) -> Cassandra
 * `claimOrResume` (LWT claim on `user_karma_coin_lookup`, `operation_type=COINS_REDEMPTION` so a
 * CREDIT and a DEBIT for the same `contextId` never collide) -> `calculateRedemption` (validates
 * `pointsToConvert <= total_earned - total_redeemed`; no partial redemption) -> `writePlan`
 * (freezes the target wallet values + a transaction id into the still-PROCESSING lookup row) ->
 * `applyPlan` (wallet -> DEBIT transaction -> lookup SUCCESS -> Redis wallet-cache refresh).
 *
 * A PROCESSING row with an already-persisted plan is resumed via `RedemptionResumeWithPlan`
 * (re-applies the same plan, never recalculates); one without a plan (a prior attempt crashed
 * before writing it) is `RedemptionProceed`, i.e. redo `calculateRedemption` from live state.
 * Mirrors [[PointsConversionHandler]]'s claimOrResume/writePlan/applyPlan design throughout.
 */
class CoinsRedemptionHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private val logger = LoggerFactory.getLogger(classOf[CoinsRedemptionHandler])

  /** Test-only seam: exposes whether/what this handler was invoked with, without adding business logic. */
  private[v2] var lastHandledEvent: Option[UnifiedEvent] = None

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val request = validateEvent(event)
    val requestKey = userKarmaCoinKey(request)
    val dedupEnabled = config.coinsRedemptionDedupEnabled

    if (dedupEnabled && !redisUtil.claimKarmaCoinDedup(requestKey, event.getJson())) {
      // Redis has already seen this exact request - skip immediately, Cassandra is not consulted.
      logger.info(s"Duplicate COINS_REDEMPTION per Redis first-level dedup, userKarmaCoinKey=$requestKey, skipping")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    try {
      claimOrResumeRedemption(request) match {
        case RedemptionAlreadySucceeded =>
          // Already logged/metered inside claimOrResume.

        case RedemptionProceed(creditDate) =>
          val calculation = try {
            calculateRedemption(request)
          } catch {
            case ex: InvalidPayloadException =>
              // Business-rule rejection - mark the lookup FAILED before letting the existing
              // DataQualityException/failed-topic path handle it.
              updateLookupStatus(request, creditDate, config.STATUS_FAILED,
                config.ADDINFO_ERROR_CODE -> config.ERROR_CODE_INSUFFICIENT_BALANCE,
                config.ADDINFO_ERROR_MESSAGE -> ex.message)
              throw ex
          }
          val plan = createAndPersistRedemptionPlan(request, calculation, creditDate)
          applyRedemptionPlan(request, event, plan)
          lastHandledEvent = Some(event)
          logger.info(s"COINS_REDEMPTION completed successfully: userId=${request.userId}, contextId=${request.contextId}, " +
            s"pointsToConvert=${request.pointsToConvert}")

        case RedemptionResumeWithPlan(plan) =>
          // Plan was already frozen by a prior crashed attempt - re-apply it as-is, never
          // recompute; safe regardless of how far that prior attempt got.
          applyRedemptionPlan(request, event, plan)
          lastHandledEvent = Some(event)
          logger.info(s"COINS_REDEMPTION resumed from persisted plan: userId=${request.userId}, " +
            s"contextId=${request.contextId}, transactionId=${plan.transactionId}")
      }
    } catch {
      case ex: Exception =>
        // Release the Redis claim before rethrowing: otherwise a Flink checkpoint replay of this
        // same event would see the stale claim, skip without ever reaching Cassandra, and
        // permanently strand the lookup row (and any persisted Frozen Plan) in PROCESSING.
        // Only relevant when dedup is enabled - if disabled, no claim was ever acquired.
        if (dedupEnabled) {
          redisUtil.releaseKarmaCoinRequestClaim(requestKey)
        }
        throw ex
    }
  }

  /** Full envelope validation for COINS_REDEMPTION, in order: userId -> operation -> actionType ->
   * pointsToConvert -> contextType -> contextId. Throws on the first failing check. */
  private[v2] def validateEvent(event: UnifiedEvent): CoinsRedemptionRequest = {
    val userId = event.dataString("userId")
    if (StringUtils.isEmpty(userId)) {
      throw InvalidUserIdException(
        s"data.userId is missing/empty for eventType=${event.eventType}, mid=${event.mid()}"
      )
    }
    val operation = event.dataString("operation")
    if (!config.OPERATION_DEBIT.equals(operation)) {
      throw InvalidPayloadException(
        s"data.operation must be '${config.OPERATION_DEBIT}' for COINS_REDEMPTION event, " +
          s"got '$operation', userId=$userId"
      )
    }
    val actionType = event.dataString("actionType")
    if (!config.ACTION_TYPE_POINTS_REDEMPTION.equals(actionType)) {
      throw InvalidPayloadException(
        s"data.actionType must be '${config.ACTION_TYPE_POINTS_REDEMPTION}' for COINS_REDEMPTION event, " +
          s"got '$actionType', userId=$userId"
      )
    }
    val pointsToConvert = event.dataLong("pointsToConvert", 0L)
    if (pointsToConvert <= 0) {
      throw InvalidPayloadException(
        s"data.pointsToConvert must be > 0 for COINS_REDEMPTION event, " +
          s"got '$pointsToConvert', userId=$userId"
      )
    }
    val contextType = event.dataString("contextType")
    if (StringUtils.isEmpty(contextType)) {
      throw MissingPayloadException(
        s"data.contextType is required for COINS_REDEMPTION event, userId=$userId"
      )
    }
    val contextId = event.dataString("contextId")
    if (StringUtils.isEmpty(contextId)) {
      throw MissingPayloadException(
        s"data.contextId is required for COINS_REDEMPTION event, userId=$userId"
      )
    }
    CoinsRedemptionRequest(userId, operation, actionType, pointsToConvert, contextType, contextId)
  }

  /** `userId|contextType|contextId` - the Redis dedup key and the Cassandra lookup's
   * `user_karma_coin_key`, same shape POINTS_CONVERSION uses; the two flows are told apart by
   * `operation_type`, not by this key. */
  private[v2] def userKarmaCoinKey(request: CoinsRedemptionRequest): String =
    request.userId + config.PIPE + request.contextType + config.PIPE + request.contextId

  /**
   * Resolves the current redemption state using the Cassandra lookup record.
   * New requests are claimed using Cassandra LWT; existing FAILED or PROCESSING requests
   * are handled according to their persisted state, while completed requests are skipped.
   * Cassandra failures are propagated to the existing Flink retry/restart mechanism.
   */
  private[v2] def claimOrResumeRedemption(request: CoinsRedemptionRequest)(implicit metrics: Metrics): RedemptionClaimOutcome = {
    val requestKey = userKarmaCoinKey(request)
    val freshCreditDate = System.currentTimeMillis()
    val freshAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)

    val claimed = cassandraUtil.claimKarmaCoinLookup(requestKey, config.EVENT_TYPE_COINS_REDEMPTION, freshCreditDate, freshAddInfo)
    if (claimed) {
      return RedemptionProceed(freshCreditDate)
    }

    // Contention: a row already exists. Read it once to find out why.
    val existing = cassandraUtil.fetchKarmaCoinLookup(requestKey, config.EVENT_TYPE_COINS_REDEMPTION)
    if (existing == null || existing.isEmpty) {
      logger.error(
        s"COINS_REDEMPTION lookup row not found after claim failed, " +
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
        logger.info(s"Duplicate COINS_REDEMPTION - SUCCESS already recorded for userKarmaCoinKey=$requestKey, skipping")
        metrics.incCounter(config.skippedEventCount)
        RedemptionAlreadySucceeded

      case s if config.STATUS_FAILED.equals(s) =>
        val newAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)
        val transitioned = cassandraUtil.transitionKarmaCoinLookup(requestKey, config.EVENT_TYPE_COINS_REDEMPTION,
          existingAddInfo, newAddInfo, freshCreditDate)
        if (!transitioned) {
          throw CassandraException(s"Could not CAS FAILED->PROCESSING for user_karma_coin_lookup key=$requestKey (contention)")
        }
        RedemptionProceed(freshCreditDate)

      case s if config.STATUS_PROCESSING.equals(s) =>
        parseRedemptionPlan(statusMap) match {
          case Some(plan) => RedemptionResumeWithPlan(plan)
          case None =>
            RedemptionProceed(existingCreditDate)
        }

      case other =>
        throw CassandraException(s"Unrecognized user_karma_coin_lookup status='$other' for key=$requestKey")
    }
  }

  /** Upserts the lookup row to `status` (+ extra addinfo fields) - replaces the whole addinfo, same
   * as [[PointsConversionHandler.updateLookupStatus]] (kept as a separate local copy - the two
   * flows' addinfo shapes diverge from here on). */
  private[v2] def updateLookupStatus(request: CoinsRedemptionRequest, creditDate: Long, status: String,
                                     extraFields: (String, Any)*)(implicit metrics: Metrics): Unit = {
    val addInfo = cassandraUtil.buildAddInfo(null, (config.STATUS -> status) +: extraFields: _*)
    cassandraUtil.updateKarmaCoinLookup(userKarmaCoinKey(request), config.EVENT_TYPE_COINS_REDEMPTION, creditDate, addInfo)
  }

  /** `(total_earned, total_redeemed)` for the user's wallet, same table POINTS_CONVERSION
   * reads/writes. No wallet row yet -> (0, 0). */
  private[v2] def readWallet(userId: String): (Int, Int) = {
    val rows = cassandraUtil.fetchKarmaCoinWallet(userId)
    if (rows != null && rows.size() > 0) (rows.get(0).getInt(config.TOTAL_EARNED), rows.get(0).getInt(config.TOTAL_REDEEMED))
    else (0, 0)
  }

  /**
   * Business calculation, happy-path only (read + validate + compute, no writes): read wallet ->
   * compute available balance -> validate the requested amount doesn't exceed it -> compute the
   * DEBIT target values.
   *
   * @throws InvalidPayloadException if `pointsToConvert` exceeds the available balance
   *                                 (`total_earned - total_redeemed`) - a business rejection, not
   *                                 an infra failure. No partial redemption - any excess rejects
   *                                 the whole request. The caller marks the lookup FAILED before
   *                                 letting this propagate.
   */
  private[v2] def calculateRedemption(request: CoinsRedemptionRequest): CoinsRedemptionCalculation = {
    val (totalEarned, totalRedeemed) = readWallet(request.userId)
    val availableCoins = totalEarned - totalRedeemed
    if (request.pointsToConvert > availableCoins) {
      throw InvalidPayloadException(
        s"data.pointsToConvert (${request.pointsToConvert}) exceeds available Karma Coin balance " +
          s"($availableCoins) for userId=${request.userId}, contextId=${request.contextId}")
    }
    val targetTotalRedeemed = totalRedeemed + request.pointsToConvert.toInt
    CoinsRedemptionCalculation(totalEarned, totalRedeemed, targetTotalRedeemed, totalEarned - targetTotalRedeemed)
  }

  /** Parses a persisted [[RedemptionPlan]] from a PROCESSING row's addinfo, if one was already
   * frozen by a prior attempt - presence of `transactionId` means a plan exists. */
  private def parseRedemptionPlan(statusMap: java.util.Map[String, Any]): Option[RedemptionPlan] = {
    if (!statusMap.containsKey(config.ADDINFO_TRANSACTION_ID)) {
      None
    } else {
      Some(RedemptionPlan(
        transactionId = statusMap.get(config.ADDINFO_TRANSACTION_ID).toString,
        createdAt = statusMap.get(config.ADDINFO_CREATED_AT).asInstanceOf[Number].longValue(),
        targetTotalEarned = statusMap.get(config.ADDINFO_TARGET_TOTAL_EARNED).asInstanceOf[Number].intValue(),
        targetTotalRedeemed = statusMap.get(config.ADDINFO_TARGET_TOTAL_REDEEMED).asInstanceOf[Number].intValue()
      ))
    }
  }

  /** Freezes the plan: persists the target wallet values and a transaction id ONCE, into the
   * still-PROCESSING lookup row's addinfo, before any wallet/transaction write is attempted.
   * `targetTotalEarned` is carried through unchanged (a DEBIT never mutates it), so [[applyRedemptionPlan]]
   * can upsert the wallet from the plan alone. */
  private[v2] def createAndPersistRedemptionPlan(request: CoinsRedemptionRequest, calculation: CoinsRedemptionCalculation,
                                                 creditDate: Long)(implicit metrics: Metrics): RedemptionPlan = {
    val plan = RedemptionPlan(
      transactionId = generateTransactionId(creditDate),
      createdAt = creditDate,
      targetTotalEarned = calculation.totalEarned,
      targetTotalRedeemed = calculation.targetTotalRedeemed
    )
    updateLookupStatus(request, creditDate, config.STATUS_PROCESSING,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId,
      config.ADDINFO_CREATED_AT -> plan.createdAt,
      config.ADDINFO_TARGET_TOTAL_EARNED -> plan.targetTotalEarned,
      config.ADDINFO_TARGET_TOTAL_REDEEMED -> plan.targetTotalRedeemed)
    plan
  }

  /** Applies a frozen plan: wallet -> DEBIT transaction -> lookup SUCCESS -> Redis refresh, in that
   * order (Cassandra first, Redis best-effort last). Every write is an absolute-value upsert or a
   * deterministic-key insert driven by the plan, so this is safe to re-run in full. No
   * `user_karma_coin_monthly_summary` write - DEBIT has no monthly cap. */
  private[v2] def applyRedemptionPlan(request: CoinsRedemptionRequest, event: UnifiedEvent, plan: RedemptionPlan)
                                     (implicit metrics: Metrics): Unit = {
    logger.info(
      s"Applying COINS_REDEMPTION plan, " +
        s"userId=${request.userId}, contextId=${request.contextId}, " +
        s"transactionId=${plan.transactionId}, " +
        s"targetTotalEarned=${plan.targetTotalEarned}, " +
        s"targetTotalRedeemed=${plan.targetTotalRedeemed}"
    )
    cassandraUtil.updateKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed)

    val balanceAfter = plan.targetTotalEarned - plan.targetTotalRedeemed
    val transactionAddInfo = cassandraUtil.buildAddInfo(null,
      config.ADDINFO_COURSE_NAME -> event.dataString("courseName"),
      config.ADDINFO_PROVIDER_NAME -> event.dataString("providerName"))
    cassandraUtil.insertKarmaCoinTransaction(request.userId, plan.createdAt, plan.transactionId, config.OPERATION_DEBIT,
      request.pointsToConvert, balanceAfter, config.ACTION_TYPE_POINTS_REDEMPTION,
      request.contextType, request.contextId, transactionAddInfo)
    logger.info(
      s"COINS_REDEMPTION transaction persisted, " +
        s"userId=${request.userId}, transactionId=${plan.transactionId}, " +
        s"amount=${request.pointsToConvert}, balanceAfter=$balanceAfter"
    )

    // updateLookupStatus always starts from an empty addinfo map, so listing only
    updateLookupStatus(request, plan.createdAt, config.STATUS_SUCCESS,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId, config.ADDINFO_POINTS_USED -> request.pointsToConvert)
    logger.info(
      s"COINS_REDEMPTION lookup marked SUCCESS, " +
        s"userId=${request.userId}, contextId=${request.contextId}, " +
        s"transactionId=${plan.transactionId}"
    )

    // DEBIT never writes user_karma_coin_monthly_summary, so reading the current month's figure
    // here (rather than hardcoding 0) avoids clobbering a legitimately-cached CREDIT value.
    val (yearMonth, convertedThisMonth) = currentPointsConvertedThisMonth(request.userId)
    redisUtil.setKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed, yearMonth, convertedThisMonth)
  }

  /** Current calendar month's `points_converted`, reused as-is from
   * [[CassandraUtil.fetchKarmaCoinMonthlySummary]] - see [[applyRedemptionPlan]] for why. No row yet -> 0. */
  private[v2] def currentPointsConvertedThisMonth(userId: String): (String, Int) = {
    val yearMonth = LocalDate.now.format(DateTimeFormatter.ofPattern(config.YYYY_DASH_MM))
    val rows = cassandraUtil.fetchKarmaCoinMonthlySummary(userId, yearMonth)
    val converted = if (rows != null && rows.size() > 0) rows.get(0).getInt(config.POINTS_CONVERTED) else 0
    (yearMonth, converted)
  }

  /** `PREFIX.timestamp.UUID`, the repo-wide id convention (same format as
   * [[PointsConversionHandler.generateTransactionId]]). Generated once in [[createAndPersistRedemptionPlan]] and
   * persisted - a replay reuses the same id from the recovered plan. */
  private[v2] def generateTransactionId(createdAt: Long): String =
    s"${config.TRANSACTION_ID_PREFIX}.$createdAt.${UUID.randomUUID().toString}"
}
