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
private[v2] case class PointsConversionRequest(userId: String, contextType: String, contextId: String,
                                               operation: String, actionType: String, pointsToConvert: Long)

/** Business calculation result - read once, reused to build the frozen plan below. */
private[v2] case class PointsConversionCalculation(lifetimeKP: Int, alreadyConvertedKP: Int, existingTotalRedeemed: Int,
                                                   unconvertedKP: Int, pointsConvertedThisMonth: Int, currentYearMonth: String,
                                                   remainingMonthlyCap: Int, maximumConvertible: Int, karmaCoinsToCredit: Long)

/**
 * The "frozen plan": computed exactly once (right after `calculateConversion` succeeds) and
 * persisted into the lookup row's `addinfo` BEFORE any wallet/monthly/transaction write is
 * attempted. Every write downstream of the plan (wallet, monthly summary, transaction) becomes an
 * absolute-value upsert driven by these fields, never a delta computed from freshly-read (and,
 * after a partial completion, possibly already-mutated) live state - so replaying the plan any
 * number of times converges to the same final state instead of double-applying. This is what
 * makes PROCESSING-recovery safe without needing to distinguish exactly how far a prior crashed
 * attempt got (wallet only? wallet+monthly? +transaction too?) - see `claimOrResume`/`applyPlan`.
 */
private[v2] case class ConversionPlan(transactionId: String, createdAt: Long, targetTotalEarned: Int,
                                      targetTotalRedeemed: Int, targetYearMonth: String, targetPointsConverted: Int)

/** Outcome of [[PointsConversionHandler.claimOrResume]]. */
private[v2] sealed trait ClaimOutcome

private[v2] case object AlreadySucceeded extends ClaimOutcome

private[v2] case class FreshAttempt(creditDate: Long) extends ClaimOutcome

private[v2] case class ResumeWithPlan(plan: ConversionPlan) extends ClaimOutcome

/**
 * Handles POINTS_CONVERSION (CREDIT) events - a user converting Karma Points into Karma Coins.
 * Payload: `data.userId`, `data.operation` ("CREDIT"), `data.actionType`, `data.pointsToConvert`,
 * `data.contextType`, `data.contextId` (mandatory, a unique per-request UUID).
 *
 * Phase 2 added full envelope validation. Phase 4 added the read-only business calculation
 * (unconverted Karma Points, monthly cap, maximum convertible). Phase 5/6 added the Cassandra
 * persistence and Redis wallet-cache refresh for the happy flow. Phase 7B adds reliability:
 *
 * Redis first-level dedup (`user|contextType|contextId`, SET NX + ~4h TTL - a duplicate here skips
 * immediately, Cassandra is never consulted; see [[RedisUtil.claimKarmaCoinDedup]]) ->
 * `claimOrResume` (LWT `INSERT ... IF NOT EXISTS` on `user_karma_coin_lookup` at LOCAL_QUORUM;
 * SUCCESS -> stop, FAILED -> CAS-retry, PROCESSING -> resume from its persisted plan if one exists) ->
 * `calculateConversion` (business-rule failure -> lookup FAILED, rethrow to the existing
 * failed-topic path, unchanged from Phase 4/5) -> `writePlan` (freezes the target wallet/monthly
 * values + transaction id, once) -> `applyPlan` (wallet -> monthly summary -> transaction ->
 * lookup SUCCESS -> Redis wallet-cache refresh, all driven by the frozen plan so replay is safe).
 *
 * Follows the same constructor/DI shape as [[FirstLoginHandler]]/[[RatingEventHandler]].
 */
class PointsConversionHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private val logger = LoggerFactory.getLogger(classOf[PointsConversionHandler])

  /** Test-only seam: exposes whether/what this handler was invoked with, without adding business logic. */
  private[v2] var lastHandledEvent: Option[UnifiedEvent] = None

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val request = validateEvent(event)

    // `config.pointsConversionDedupEnabled &&` short-circuits: when false,
    // claimKarmaCoinRequest is never called at all, and Cassandra's claimOrResume below always runs.
    if (config.pointsConversionDedupEnabled && !redisUtil.claimKarmaCoinDedup(userKarmaCoinKey(request), event.getJson())) {
      // First-level dedup hit: Redis has seen this exact request key within the TTL window.
      // Skip immediately, per the confirmed design - do not consult Cassandra for this case.
      logger.info(s"Duplicate POINTS_CONVERSION per Redis first-level dedup, userKarmaCoinKey=${userKarmaCoinKey(request)}, skipping")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    claimOrResume(request) match {
      case AlreadySucceeded =>
        // Already logged/metered inside claimOrResume.

      case FreshAttempt(creditDate) =>
        val calculation = try {
          calculateConversion(request)
        } catch {
          case ex: InvalidPayloadException =>
            // Business-rule rejection. Mark the lookup FAILED before letting the existing
            // DataQualityException/failed-topic path handle it - no new Kafka failure mechanism.
            logger.warn(
              s"POINTS_CONVERSION rejected, userId=${request.userId}, " +
                s"pointsToConvert=${request.pointsToConvert}, " +
                s"creditDate=$creditDate, reason=${ex.message}"
            )
            updateLookupStatus(request, creditDate, config.STATUS_FAILED,
              config.ADDINFO_ERROR_CODE -> config.ERROR_CODE_CONVERSION_LIMIT_EXCEEDED,
              config.ADDINFO_ERROR_MESSAGE -> ex.message)
            throw ex
        }
        val plan = freezeConversionPlan(request, calculation, creditDate)
        applyConversionPlan(request, plan)
        lastHandledEvent = Some(event)
        logger.info(s"POINTS_CONVERSION completed successfully: userId=${request.userId}, contextId=${request.contextId}, " +
          s"pointsToConvert=${request.pointsToConvert}, karmaCoinsToCredit=${calculation.karmaCoinsToCredit}")

      case ResumeWithPlan(plan) =>
        // A plan was already frozen by a prior (now-dead, crashed) attempt - do NOT recompute or
        // re-validate against live state (it may already reflect a partially-applied prior
        // attempt); just re-apply the same plan. Idempotent regardless of how far that prior
        // attempt got (see ConversionPlan's doc).
        applyConversionPlan(request, plan)
        lastHandledEvent = Some(event)
        logger.info(s"POINTS_CONVERSION resumed and completed from a persisted plan: userId=${request.userId}, " +
          s"contextId=${request.contextId}, transactionId=${plan.transactionId}")
    }
  }

  /**
   * All incoming-event validation for POINTS_CONVERSION, in the mandated order - userId ->
   * contextType -> contextId -> operation -> actionType -> pointsToConvert. Throws a
   * [[org.sunbird.job.karmapoints.v2.exceptions.DataQualityException]] subtype on the first check
   * that fails; returns the validated fields otherwise. Runs BEFORE any claim, so these failures
   * never touch Redis or `user_karma_coin_lookup` at all.
   */
  private[v2] def validateEvent(event: UnifiedEvent): PointsConversionRequest = {
    val userId = event.dataString("userId")
    if (StringUtils.isEmpty(userId)) {
      throw InvalidUserIdException(s"data.userId is missing/empty for eventType=${event.eventType}, mid=${event.mid()}")
    }
    val contextType = event.dataString("contextType")
    if (StringUtils.isEmpty(contextType)) {
      throw MissingPayloadException(s"data.contextType is required for POINTS_CONVERSION event, userId=$userId")
    }
    if (!config.EVENT_TYPE_POINTS_CONVERSION.equals(contextType)) {
      throw InvalidPayloadException(
        s"data.contextType must be '${config.EVENT_TYPE_POINTS_CONVERSION}' for POINTS_CONVERSION event, got '$contextType', userId=$userId")
    }
    val contextId = event.dataString("contextId")
    if (StringUtils.isEmpty(contextId)) {
      throw MissingPayloadException(s"data.contextId is required for POINTS_CONVERSION event, userId=$userId")
    }

    val operation = event.dataString("operation")
    if (!config.OPERATION_CREDIT.equals(operation)) {
      throw InvalidPayloadException(
        s"data.operation must be '${config.OPERATION_CREDIT}' for POINTS_CONVERSION event, got '$operation', userId=$userId")
    }
    val actionType = event.dataString("actionType")
    if (!config.EVENT_TYPE_POINTS_CONVERSION.equals(actionType)) {
      throw InvalidPayloadException(
        s"data.actionType must be '${config.EVENT_TYPE_POINTS_CONVERSION}' for POINTS_CONVERSION event, got '$actionType', userId=$userId")
    }
    val pointsToConvert = event.dataLong("pointsToConvert", 0L)
    if (pointsToConvert <= 0) {
      throw InvalidPayloadException(
        s"data.pointsToConvert must be > 0 for POINTS_CONVERSION event, got '$pointsToConvert', userId=$userId")
    }

    PointsConversionRequest(userId, contextType, contextId, operation, actionType, pointsToConvert)
  }

  /** `userId|contextType|contextId` - the Karma Coin business key: the Redis dedup key AND the
   * Cassandra lookup's `user_karma_coin_key`, so both layers key on the exact same string. */
  private[v2] def userKarmaCoinKey(request: PointsConversionRequest): String =
    request.userId + config.PIPE + request.contextType + config.PIPE + request.contextId

  /**
   * Claims (or resumes, or rejects) processing for this request against `user_karma_coin_lookup`,
   * via an atomic `INSERT ... IF NOT EXISTS` at LOCAL_QUORUM - the sole LWT operation in this
   * handler. `PROCESSING`/`FAILED` recovery, when needed, costs at most ONE extra read (the
   * existing row, returned by [[CassandraUtil.fetchKarmaCoinLookup]] since a failed conditional
   * INSERT doesn't surface the pre-existing row's columns through the Statement-based `update()`
   * jobs-core exposes) - never more, and NEVER an extra read once a persisted plan is found (that
   * case returns directly, no further Cassandra access here).
   *
   * A Cassandra failure anywhere in this method is NOT treated as any of the outcomes below - it
   * propagates as [[CassandraException]] (a SystemException) exactly like every other Cassandra
   * write in V2, restarting the job rather than silently proceeding without the claim.
   */
  private[v2] def claimOrResume(request: PointsConversionRequest)(implicit metrics: Metrics): ClaimOutcome = {
    val key = userKarmaCoinKey(request)
    val freshCreditDate = System.currentTimeMillis()
    val freshAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)

    val claimed = cassandraUtil.claimKarmaCoinLookup(key, config.EVENT_TYPE_POINTS_CONVERSION, freshCreditDate, freshAddInfo)
    if (claimed) {
      return FreshAttempt(freshCreditDate)
    }

    // Contention: a row already exists. Read it once to find out why.
    val existing = cassandraUtil.fetchKarmaCoinLookup(key, config.EVENT_TYPE_POINTS_CONVERSION)
    if (existing == null || existing.isEmpty) {
      // Vanishingly unlikely (claim just failed because the row existed, yet it's now gone) -
      // treat conservatively as a system hiccup rather than guessing; replay will retry cleanly.
      logger.error(
        s"POINTS_CONVERSION lookup row not found after claim failed, " +
          s"userKarmaCoinKey=$key. The row may have disappeared between claim and read."
      )
      throw CassandraException(s"user_karma_coin_lookup row for key=$key disappeared between claim and read")
    }
    val existingAddInfo = existing.get(0).getString(config.ADD_INFO)
    val existingCreditDate = existing.get(0).getLong(config.DB_COLUMN_CREDIT_DATE)
    val statusMap = if (StringUtils.isEmpty(existingAddInfo)) new java.util.HashMap[String, Any]()
    else JSONUtil.deserialize[java.util.Map[String, Any]](existingAddInfo)
    val status = statusMap.getOrDefault(config.STATUS, config.EMPTY).asInstanceOf[String]

    status match {
      case s if config.STATUS_SUCCESS.equals(s) =>
        logger.info(s"Duplicate POINTS_CONVERSION - SUCCESS already recorded for userKarmaCoinKey=$key, skipping")
        metrics.incCounter(config.skippedEventCount)
        AlreadySucceeded

      case s if config.STATUS_FAILED.equals(s) =>
        // Allow a new attempt: CAS-transition FAILED -> PROCESSING using the exact addinfo value
        // just read as the expected precondition.
        val newAddInfo = cassandraUtil.buildAddInfo(null, config.STATUS -> config.STATUS_PROCESSING)
        val transitioned = cassandraUtil.transitionKarmaCoinLookup(key, config.EVENT_TYPE_POINTS_CONVERSION,
          existingAddInfo, newAddInfo, freshCreditDate)
        if (!transitioned) {
          // Someone else changed the row between our read and this CAS attempt - vanishingly rare
          // under keyBy(userId); fail safe rather than loop inline, replay will retry cleanly.
          throw CassandraException(s"Could not CAS FAILED->PROCESSING for user_karma_coin_lookup key=$key (contention)")
        }
        FreshAttempt(freshCreditDate)

      case s if config.STATUS_PROCESSING.equals(s) =>
        parsePlan(statusMap) match {
          case Some(plan) => ResumeWithPlan(plan)
          case None =>
            // A prior attempt claimed PROCESSING but crashed before the plan was written - nothing
            // else was ever persisted for this request. Safe to redo from calculateConversion.
            FreshAttempt(existingCreditDate)
        }

      case other =>
        logger.error(
          s"Unrecognized POINTS_CONVERSION lookup status='$other', " +
            s"userKarmaCoinKey=$key. Cannot safely continue processing."
        )
        throw CassandraException(s"Unrecognized user_karma_coin_lookup status='$other' for key=$key")
    }
  }

  private def parsePlan(statusMap: java.util.Map[String, Any]): Option[ConversionPlan] = {
    if (!statusMap.containsKey(config.ADDINFO_TRANSACTION_ID)) {
      None
    } else {
      // .asInstanceOf[Number] (not .toString.toInt/toLong) - safe regardless of whether the JSON
      // library boxed a given field as Integer/Long/BigInteger; all java.lang.Number subtypes
      // implement intValue()/longValue() without a numeric-format parse that could throw.
      Some(ConversionPlan(
        transactionId = statusMap.get(config.ADDINFO_TRANSACTION_ID).toString,
        createdAt = statusMap.get(config.ADDINFO_CREATED_AT).asInstanceOf[Number].longValue(),
        targetTotalEarned = statusMap.get(config.ADDINFO_TARGET_TOTAL_EARNED).asInstanceOf[Number].intValue(),
        targetTotalRedeemed = statusMap.get(config.ADDINFO_TARGET_TOTAL_REDEEMED).asInstanceOf[Number].intValue(),
        targetYearMonth = statusMap.get(config.ADDINFO_TARGET_YEAR_MONTH).toString,
        targetPointsConverted = statusMap.get(config.ADDINFO_TARGET_POINTS_CONVERTED).asInstanceOf[Number].intValue()
      ))
    }
  }

  /** Upserts the lookup row to `status` (+ any extra addinfo fields, e.g. errorCode/errorMessage for
   * FAILED or transactionId/pointsConverted for SUCCESS). Plain upsert - ownership was already
   * established by `claimOrResume`'s conditional write, so no further LWT is needed here.
   *
   * Always calls `buildAddInfo(null, ...)` - i.e. every call REPLACES the row's addinfo entirely
   * rather than merging with whatever is currently there. This is deliberate: it's what makes each
   * transition (claim -> plan -> SUCCESS, or claim -> FAILED) end up with exactly the fields that
   * transition's caller lists and nothing else, without a separate "remove these keys" step. */
  private[v2] def updateLookupStatus(request: PointsConversionRequest, creditDate: Long, status: String,
                                     extraFields: (String, Any)*)(implicit metrics: Metrics): Unit = {
    val addInfo = cassandraUtil.buildAddInfo(null, (config.STATUS -> status) +: extraFields: _*)
    cassandraUtil.updateKarmaCoinLookup(userKarmaCoinKey(request), config.EVENT_TYPE_POINTS_CONVERSION, creditDate, addInfo)
  }

  /**
   * Business calculation, happy-path only (read + validate + compute - no writes). Order matches
   * the mandated flow: Karma Points summary -> wallet -> unconverted KP -> monthly summary ->
   * remaining cap -> maximum convertible -> validate requested amount -> conversion result.
   *
   * @throws InvalidPayloadException if `request.pointsToConvert` exceeds the maximum convertible
   *                                 amount (insufficient unconverted points and/or monthly cap
   *                                 reached) - a business rejection, not an infra failure; the
   *                                 caller (`doHandle`) is responsible for marking the lookup
   *                                 FAILED before letting this propagate.
   */
  private[v2] def calculateConversion(request: PointsConversionRequest): PointsConversionCalculation = {
    val lifetimeKP = readPointsSummary(request.userId)
    val (alreadyConvertedKP, existingTotalRedeemed) = readWallet(request.userId)
    val unconvertedKP = calculateUnconvertedKP(lifetimeKP, alreadyConvertedKP)

    val currentYearMonth = LocalDate.now.format(DateTimeFormatter.ofPattern(config.YYYY_DASH_MM))
    val pointsConvertedThisMonth = readMonthlySummary(request.userId, currentYearMonth)
    val remainingMonthlyCap = calculateRemainingMonthlyCap(pointsConvertedThisMonth)

    val maximumConvertible = math.min(unconvertedKP, remainingMonthlyCap)
    validateConversionAmount(request, maximumConvertible)
    logger.info(
      s"POINTS_CONVERSION calculation completed: userId=${request.userId}, " +
        s"requestedPoints=${request.pointsToConvert}, unconvertedKP=$unconvertedKP, " +
        s"remainingMonthlyCap=$remainingMonthlyCap, maximumConvertible=$maximumConvertible"
    )
    PointsConversionCalculation(lifetimeKP, alreadyConvertedKP, existingTotalRedeemed, unconvertedKP,
      pointsConvertedThisMonth, currentYearMonth, remainingMonthlyCap, maximumConvertible, calculateCoins(request.pointsToConvert))


  }

  /** Lifetime Karma Points, reusing the existing `user_karma_points_summary` read as-is (same
   * consistency level as every other Karma Points reader of this table - not bumped to
   * LOCAL_QUORUM here, since that table/method is shared with the 7 existing Karma Points
   * handlers and changing it would change their behavior too). No row -> 0. */
  private[v2] def readPointsSummary(userId: String): Int = {
    val rows = cassandraUtil.fetchUserKpSummary(userId)
    if (rows != null && rows.size() > 0) rows.get(0).getInt(config.TOTAL_POINTS) else 0
  }

  /** `(total_earned, total_redeemed)`. Already-converted Karma Points, per the agreed 1:1 ratio
   * rule, is `total_earned`; `total_redeemed` is carried through for the wallet write and
   * transaction `balance_after` later, so the wallet is only read once per event. No wallet row
   * yet (never converted/redeemed) -> (0, 0). */
  private[v2] def readWallet(userId: String): (Int, Int) = {
    val rows = cassandraUtil.fetchKarmaCoinWallet(userId)
    if (rows != null && rows.size() > 0) (rows.get(0).getInt(config.TOTAL_EARNED), rows.get(0).getInt(config.TOTAL_REDEEMED))
    else (0, 0)
  }

  private[v2] def calculateUnconvertedKP(lifetimeKP: Int, alreadyConvertedKP: Int): Int =
    math.max(0, lifetimeKP - alreadyConvertedKP)

  /** Karma Points already converted to coins in `yearMonth`. No monthly-summary row yet -> 0,
   * per the agreed behavior for a month with no conversions so far. */
  private[v2] def readMonthlySummary(userId: String, yearMonth: String): Int = {
    val rows = cassandraUtil.fetchKarmaCoinMonthlySummary(userId, yearMonth)
    if (rows != null && rows.size() > 0) rows.get(0).getInt(config.POINTS_CONVERTED) else 0
  }

  /** `config.pointsConversionMonthlyLimit` (Phase 1) is the single source of truth for the monthly
   * cap - never hardcoded here. */
  private[v2] def calculateRemainingMonthlyCap(pointsConvertedThisMonth: Int): Int =
    math.max(0, config.pointsConversionMonthlyLimit - pointsConvertedThisMonth)

  private[v2] def validateConversionAmount(request: PointsConversionRequest, maximumConvertible: Int): Unit = {
    if (request.pointsToConvert > maximumConvertible) {
      throw InvalidPayloadException(
        s"data.pointsToConvert (${request.pointsToConvert}) exceeds the maximum convertible amount " +
          s"($maximumConvertible) for userId=${request.userId}, contextId=${request.contextId}")
    }
  }

  /** 1 Karma Point = 1 Karma Coin (current ratio). Kept as its own method so a future
   * configurable/non-1:1 ratio only changes this one place. */
  private[v2] def calculateCoins(pointsToConvert: Long): Long = pointsToConvert

  /**
   * Freezes the plan: computes the exact target wallet/monthly-summary values and a transaction id
   * ONCE, and persists them into the (still-PROCESSING) lookup row's addinfo before any
   * wallet/monthly/transaction write is attempted. This is the write that makes everything
   * downstream idempotent under replay (see [[ConversionPlan]]'s doc).
   */
  private[v2] def freezeConversionPlan(request: PointsConversionRequest, calculation: PointsConversionCalculation,
                                       creditDate: Long)(implicit metrics: Metrics): ConversionPlan = {
    val plan = ConversionPlan(
      transactionId = generateTransactionId(creditDate),
      createdAt = creditDate,
      targetTotalEarned = calculation.alreadyConvertedKP + request.pointsToConvert.toInt,
      targetTotalRedeemed = calculation.existingTotalRedeemed,
      targetYearMonth = calculation.currentYearMonth,
      targetPointsConverted = calculation.pointsConvertedThisMonth + request.pointsToConvert.toInt
    )
    updateLookupStatus(request, creditDate, config.STATUS_PROCESSING,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId,
      config.ADDINFO_CREATED_AT -> plan.createdAt,
      config.ADDINFO_TARGET_TOTAL_EARNED -> plan.targetTotalEarned,
      config.ADDINFO_TARGET_TOTAL_REDEEMED -> plan.targetTotalRedeemed,
      config.ADDINFO_TARGET_YEAR_MONTH -> plan.targetYearMonth,
      config.ADDINFO_TARGET_POINTS_CONVERTED -> plan.targetPointsConverted)
    logger.info(
      s"POINTS_CONVERSION plan frozen, userId=${request.userId}, " +
        s"transactionId=${plan.transactionId}, " +
        s"targetTotalEarned=${plan.targetTotalEarned}, " +
        s"targetPointsConverted=${plan.targetPointsConverted}, " +
        s"yearMonth=${plan.targetYearMonth}"
    )
    plan
  }

  /**
   * Applies a frozen plan: wallet -> monthly summary -> transaction -> lookup SUCCESS -> Redis
   * refresh, in that order (Cassandra, source of truth, first; Redis best-effort, last). Every
   * Cassandra write here is an absolute-value upsert (or, for the transaction, an insert with a
   * fully deterministic primary key) driven by the plan - safe to re-run in full regardless of how
   * much of it a previous attempt already completed, so no probing/branching on which step was
   * already done is needed.
   */
  private[v2] def applyConversionPlan(request: PointsConversionRequest, plan: ConversionPlan)(implicit metrics: Metrics): Unit = {
    logger.info(
      s"Applying POINTS_CONVERSION plan, userId=${request.userId}, " +
        s"transactionId=${plan.transactionId}, " +
        s"targetTotalEarned=${plan.targetTotalEarned}, " +
        s"targetPointsConverted=${plan.targetPointsConverted}"
    )
    cassandraUtil.updateKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed)
    cassandraUtil.updateKarmaCoinMonthlySummary(request.userId, plan.targetYearMonth, plan.targetPointsConverted, System.currentTimeMillis())

    val balanceAfter = plan.targetTotalEarned - plan.targetTotalRedeemed
    val transactionAddInfo = cassandraUtil.buildAddInfo(null,
      config.ADDINFO_POINTS_USED -> request.pointsToConvert,
      config.ADDINFO_RATIO -> config.RATIO_ONE_TO_ONE)
    cassandraUtil.insertKarmaCoinTransaction(request.userId, plan.createdAt, plan.transactionId, config.OPERATION_CREDIT,
      calculateCoins(request.pointsToConvert), balanceAfter, config.EVENT_TYPE_POINTS_CONVERSION,
      request.contextType, request.contextId, transactionAddInfo)

    updateLookupStatus(request, plan.createdAt, config.STATUS_SUCCESS,
      config.ADDINFO_TRANSACTION_ID -> plan.transactionId, config.ADDINFO_POINTS_CONVERTED -> request.pointsToConvert)

    redisUtil.setKarmaCoinWallet(request.userId, plan.targetTotalEarned, plan.targetTotalRedeemed,
      plan.targetYearMonth, plan.targetPointsConverted)
    logger.info(
      s"POINTS_CONVERSION completed, userId=${request.userId}, " +
        s"transactionId=${plan.transactionId}, points=${request.pointsToConvert}"
    )
  }

  /** `PREFIX.timestamp.UUID`, the repo-wide id convention (e.g.
   * `ActivityAggregatesFunctionV2`'s `s"LP.$ets.${UUID.randomUUID()}"`). Generated once (in
   * [[freezeConversionPlan]]) and persisted - a replay reuses the same id from the recovered plan rather
   * than generating a new one, which is what makes the transaction insert idempotent. */
  private[v2] def generateTransactionId(createdAt: Long): String =
    s"${config.TRANSACTION_ID_PREFIX}.$createdAt.${UUID.randomUUID().toString}"
}
