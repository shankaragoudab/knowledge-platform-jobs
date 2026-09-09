package org.sunbird.job.karmapoints.v2.storage

import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select, Update}
import com.datastax.driver.core.{ConsistencyLevel, Row}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.exceptions.{CassandraException, InvalidUserException}
import org.sunbird.job.util.{JSONUtil, CassandraUtil => JobsCoreCassandraUtil}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * V2 storage layer for karma-points Cassandra access. Same tables/columns as V1's
 * `karma-points-persist-processor` `Utility` object (no schema changes) - business logic that
 * decided *when* to call these (point amounts, ACBP decay, etc.) now lives in the handlers, this
 * class only knows how to read/write rows and translate driver failures into [[CassandraException]]
 * so the 2-path error strategy in KarmaPointsProcessorFnV2 can tell data-quality apart from infra failures.
 */
class CassandraUtil(config: KarmaPointsV2Config, cassandraUtil: JobsCoreCassandraUtil) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CassandraUtil])
  private lazy val mapper: ObjectMapper = new ObjectMapper()

  /**
   * Transient/connection/timeout driver exceptions - the request never reached a coordinator
   * (`NoHostAvailableException`), the connection itself failed or a client-side wait expired
   * (`ConnectionException`, which also covers its subclasses `TransportException` and
   * `OperationTimedOutException`), the local connection pool was saturated
   * (`BusyConnectionException`), the coordinator shed load (`OverloadedException`), too few
   * replicas were reachable at request time (`UnavailableException`), or a write/read got no
   * (or an incomplete) reply from replicas in time (`WriteTimeoutException`/
   * `ReadTimeoutException`) - a retry has a genuine chance of reaching a different/recovered host.
   * Every other [[DriverException]] (`SyntaxError`, `InvalidQueryException`,
   * `AlreadyExistsException`, `WriteFailureException`/`ReadFailureException` - an explicit failure
   * response, not a timeout - etc.) means the identical request would fail identically again, so
   * NOT retried here.
   */
  private def isTransientDriverError(ex: DriverException): Boolean = ex match {
    case _: NoHostAvailableException => true
    case _: ConnectionException => true
    case _: BusyConnectionException => true
    case _: OverloadedException => true
    case _: UnavailableException => true
    case _: WriteTimeoutException => true
    case _: ReadTimeoutException => true
    case _ => false
  }

  /**
   * Every Cassandra call in this class runs through here. On a transient/connection/timeout
   * failure (see [[isTransientDriverError]]), `f` is re-invoked exactly ONCE - safe because every
   * write in this class is either an absolute-value upsert or a Frozen-Plan-driven insert with a
   * deterministic key (see [[updateKarmaCoinWallet]]/[[insertKarmaCoinTransaction]]/
   * [[updateKarmaCoinLookup]]'s docs), so re-running the identical already-built statement is a
   * no-op on a partial prior success, never a double-apply; `f`'s closure already captures those
   * exact values; the retry re-executes them as-is, never recomputes anything. A non-transient
   * DriverException, or a second transient failure on the retry, propagates as [[CassandraException]]
   * exactly as before - the existing SystemException/restart path is unchanged either way.
   */
  private def guard[T](opName: String)(f: => T): T = {
    try f catch {
      case ex: DriverException if isTransientDriverError(ex) =>
        logger.warn(s"Cassandra operation failed with a transient error, retrying once: $opName", ex)
        try f catch {
          case retryEx: DriverException =>
            logger.error(s"Cassandra operation failed on retry: $opName", retryEx)
            throw CassandraException(s"Cassandra operation failed: $opName", Some(retryEx))
        }
      case ex: DriverException =>
        logger.error(s"Cassandra operation failed: $opName", ex)
        throw CassandraException(s"Cassandra operation failed: $opName", Some(ex))
    }
  }

  def close(): Unit = cassandraUtil.close()

  // ---- Reads ----

  def fetchContentHierarchy(courseId: String)(implicit metrics: Metrics): util.HashMap[String, AnyRef] = guard("fetchContentHierarchy") {
    val query: Select.Where = QueryBuilder
      .select(config.HIERARCHY)
      .from(config.content_hierarchy_KeySpace, config.content_hierarchy_table)
      .where(QueryBuilder.eq(config.IDENTIFIER, courseId))
    metrics.incCounter(config.dbReadCount)
    val rows = cassandraUtil.find(query.toString)
    if (rows != null && rows.size() > 0) {
      val hierarchy = rows.get(0).getString(config.HIERARCHY)
      try {
        mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]]).asInstanceOf[util.HashMap[String, AnyRef]]
      } catch {
        case e: Exception =>
          logger.error(s"Failed to parse hierarchy JSON for courseId: $courseId", e)
          throw e
      }
    } else new util.HashMap[String, AnyRef]()
  }

  def doesAssessmentExistInHierarchy(hierarchy: java.util.Map[String, AnyRef]): String = {
    val childrenMap = hierarchy.get(config.CHILDREN).asInstanceOf[util.ArrayList[util.HashMap[String, AnyRef]]]
    if (childrenMap == null) return config.EMPTY
    for (children <- childrenMap) {
      if (children.get(config.PRIMARY_CATEGORY) == config.COURSE_ASSESSMENT) {
        return children.get(config.IDENTIFIER).asInstanceOf[String]
      }
    }
    config.EMPTY
  }

  def fetchUserAssessmentResult(userId: String, assessmentId: String): util.List[Row] = guard("fetchUserAssessmentResult") {
    val query: Select = QueryBuilder.select(config.DB_COLUMN_SUBMIT_ASSESSMENT_RESPONSE)
      .from(config.sunbird_keyspace, config.user_assessment_data_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
      .and(QueryBuilder.eq(config.DB_COLUMN_ASSESSMENT_ID, assessmentId)).limit(1)
    cassandraUtil.find(query.toString)
  }

  def fetchUserKarmaPointsCreditLookup(userId: String, contextType: String, operationType: String, contextId: String): util.List[Row] =
    guard("fetchUserKarmaPointsCreditLookup") {
      val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
      query.where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
      cassandraUtil.find(query.toString)
    }

  /**
   * Karma Coin idempotency lookup - reads `user_karma_coin_lookup`, a SEPARATE table from
   * `user_karma_points_credit_lookup` above (different key/columns, different domain).
   *
   * Decision-critical (this is the idempotency gate) - LOCAL_QUORUM, and executed via
   * `findAllWithStatement` (preserves the Statement object, and with it the consistency level) NOT
   * `find(query.toString)` as elsewhere in this class - a stringified query loses any
   * `setConsistencyLevel` call before it ever reaches the driver.
   */
  def fetchKarmaCoinLookup(userKarmaCoinKey: String, operationType: String): util.List[Row] =
    guard("fetchKarmaCoinLookup") {
      val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_coin_lookup_table)
      query.where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_COIN_KEY, userKarmaCoinKey))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.findAllWithStatement(query)
    }

  /** Karma Coin wallet - `total_earned`/`total_redeemed` for the given user. Empty if the user has
   * no wallet row yet (never earned/redeemed a Karma Coin). Decision-critical - LOCAL_QUORUM. */
  def fetchKarmaCoinWallet(userId: String): util.List[Row] = guard("fetchKarmaCoinWallet") {
    val query: Select = QueryBuilder.select(config.TOTAL_EARNED, config.TOTAL_REDEEMED)
      .from(config.sunbird_keyspace, config.user_karma_coin_wallet_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.findAllWithStatement(query)
  }

  /** Karma Coin monthly conversion summary - `points_converted` for the given user+yearMonth.
   * Empty if nothing has been converted yet in that month. Decision-critical - LOCAL_QUORUM. */
  def fetchKarmaCoinMonthlySummary(userId: String, yearMonth: String): util.List[Row] = guard("fetchKarmaCoinMonthlySummary") {
    val query: Select = QueryBuilder.select(config.POINTS_CONVERTED)
      .from(config.sunbird_keyspace, config.user_karma_coin_monthly_summary_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
      .and(QueryBuilder.eq(config.YEAR_MONTH, yearMonth))
    query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.findAllWithStatement(query)
  }

  def fetchUserKarmaPoints(creditDate: Date, userId: String, contextType: String, operationType: String, contextId: String): util.List[Row] =
    guard("fetchUserKarmaPoints") {
      val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_table)
      query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
        .and(QueryBuilder.eq(config.DB_COLUMN_CREDIT_DATE, creditDate))
        .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_TYPE, contextType))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
        .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_ID, contextId))
      cassandraUtil.find(query.toString)
    }

  /**
   * Idempotency gate: a `user_karma_points_credit_lookup` row for this key+operationType means the
   * event was already credited, regardless of `user_karma_points` state - checking the points table
   * here would defeat the purpose of the lookup table as the single source of truth for "already
   * processed", so this intentionally only queries the lookup table.
   */
  def doesEntryExist(userId: String, contextType: String, operationType: String, contextId: String): Boolean = {
    val lookup = fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, contextId)
    lookup != null && lookup.size() > 0
  }

  def hasEarnedFirstEnrolmentPoints(userId: String): Boolean = guard("hasEarnedFirstEnrolmentPoints") {
    val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    val rows = cassandraUtil.find(query.toString)
    rows != null && rows.exists(row =>
      config.OPERATION_TYPE_ENROLMENT.equals(row.getString(config.DB_COLUMN_OPERATION_TYPE)) && row.getInt(config.POINTS) > 0)
  }

  def hasReachedNonACBPMonthlyCutOff(userId: String): Boolean = {
    val (_, infoMap) = readSummary(userId)
    val currentDateStr = LocalDate.now.format(DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM))
    var quotaCount = 0
    val currStr = infoMap.get(config.FORMATTED_MONTH)
    if (currStr != null && currentDateStr.equals(currStr)) {
      quotaCount = infoMap.getOrDefault(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, Integer.valueOf(0)).asInstanceOf[Int]
    }
    quotaCount >= config.nonAcbpCourseQuota
  }

  /** Existence check for the given user id, per the storage API contract (no throw on absence). */
  def getUserExists(userId: String): Boolean = guard("getUserExists") {
    val query: Select = QueryBuilder.select(config.identifier).from(config.sunbird_keyspace, config.user_table)
    query.where(QueryBuilder.eq(config.ID, userId.trim))
    val rows = cassandraUtil.find(query.toString)
    rows != null && rows.size() > 0
  }

  /** Same lookup as [[getUserExists]] but returns the root-org-id needed for CB-Plan headers, throwing
   * InvalidUserException (data-quality, not infra) when the user row genuinely doesn't exist. */
  def fetchUserRootOrgId(userId: String): String = guard("fetchUserRootOrgId") {
    val query: Select = QueryBuilder.select(config.ROOT_ORG_ID).from(config.sunbird_keyspace, config.user_table)
    query.where(QueryBuilder.eq(config.ID, userId.trim))
    val rows = cassandraUtil.find(query.toString)
    if (rows == null || rows.size() < 1) {
      throw InvalidUserException(s"No user record found for userId: $userId")
    }
    rows.get(0).getString(config.ROOT_ORG_ID)
  }

  def fetchUserBatch(courseId: String, batchId: String): util.List[Row] = guard("fetchUserBatch") {
    val query: Select = QueryBuilder.select().from(config.sunbird_courses_keyspace, config.course_batch_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_COURSE_ID, courseId)).and(QueryBuilder.eq(config.DB_COLUMN_BATCH_ID, batchId))
    cassandraUtil.find(query.toString)
  }

  def fetchUserKpSummary(userId: String): util.List[Row] = guard("fetchUserKpSummary") {
    val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_summary_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    cassandraUtil.find(query.toString)
  }

  private def readSummary(userId: String): (Int, java.util.Map[String, Any]) = {
    val rows = fetchUserKpSummary(userId)
    if (rows.size() > 0) {
      val total = rows.get(0).getInt(config.TOTAL_POINTS)
      val info = rows.get(0).getString(config.ADD_INFO)
      val infoMap = if (StringUtils.isEmpty(info)) new util.HashMap[String, Any]() else JSONUtil.deserialize[java.util.HashMap[String, Any]](info)
      (total, infoMap)
    } else (0, new util.HashMap[String, Any]())
  }

  // ---- Writes ----

  def buildAddInfo(existingAddInfo: String, updates: (String, Any)*): String = {
    val infoMap: java.util.Map[String, Any] = if (StringUtils.isEmpty(existingAddInfo)) new util.HashMap[String, Any]()
    else JSONUtil.deserialize[java.util.HashMap[String, Any]](existingAddInfo)
    updates.foreach { case (key, value) => infoMap.put(key, value.asInstanceOf[AnyRef]) }
    mapper.writeValueAsString(infoMap)
  }

  // ---- Karma Coin writes ----
  // Decision-critical: LOCAL_QUORUM on every statement, executed via the Statement-preserving
  // jobs-core methods (`update(Statement)`), never `.toString()` + `upsert(String)` - a stringified
  // query silently drops any consistency level (and any `.ifNotExists()`/`.onlyIf()` condition would
  // still apply, but there'd be no reason to lose the consistency level for no benefit).

  /**
   * Atomically claims `user_karma_coin_lookup` via `INSERT ... IF NOT EXISTS` at LOCAL_QUORUM -
   * the only LWT operation in this class (and the first LWT usage anywhere in this codebase).
   * `wasApplied()` is exposed exactly as-is by jobs-core's `CassandraUtil.update(Statement): Boolean`
   * (`session.execute(stmt); rs.wasApplied()`) - no jobs-core change needed for this part.
   *
   * @return true if this call created the row (genuinely fresh claim); false if a row already
   *         existed - the caller must then read it (via [[fetchKarmaCoinLookup]]) to branch on its
   *         current status, since a failed conditional INSERT's ResultSet with the existing row's
   *         columns isn't surfaced through `update(Statement): Boolean`.
   */
  def claimKarmaCoinLookup(userKarmaCoinKey: String, operationType: String, creditDate: Long, addInfo: String)
                          (implicit metrics: Metrics): Boolean = guard("claimKarmaCoinLookup") {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_coin_lookup_table)
      .value(config.DB_COLUMN_USER_KARMA_COIN_KEY, userKarmaCoinKey)
      .value(config.DB_COLUMN_OPERATION_TYPE, operationType)
      .value(config.DB_COLUMN_CREDIT_DATE, creditDate)
      .value(config.ADD_INFO, addInfo)
      .ifNotExists()
    query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    val applied = cassandraUtil.update(query)
    if (applied) metrics.incCounter(config.dbUpdateCount)
    applied
  }

  /**
   * CAS-transitions a lookup row from a known prior `addinfo` value to a new one (used for
   * FAILED -> PROCESSING retries) via `UPDATE ... IF addinfo = <expected>` at LOCAL_QUORUM.
   *
   * @return true if the transition was applied (expected still matched); false if someone else
   *         changed the row between the caller's read and this call (rare under `keyBy(userId)`).
   */
  def transitionKarmaCoinLookup(userKarmaCoinKey: String, operationType: String, expectedAddInfo: String,
                                newAddInfo: String, creditDate: Long)(implicit metrics: Metrics): Boolean =
    guard("transitionKarmaCoinLookup") {
      val query: Update.Where = QueryBuilder.update(config.sunbird_keyspace, config.user_karma_coin_lookup_table)
        .`with`(QueryBuilder.set(config.ADD_INFO, newAddInfo))
        .and(QueryBuilder.set(config.DB_COLUMN_CREDIT_DATE, creditDate))
        .where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_COIN_KEY, userKarmaCoinKey))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
      query.onlyIf(QueryBuilder.eq(config.ADD_INFO, expectedAddInfo))
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      val applied = cassandraUtil.update(query)
      if (applied) metrics.incCounter(config.dbUpdateCount)
      applied
    }

  /**
   * Plain (non-conditional) upsert of the lookup row's `addinfo`/`credit_date` - used once
   * ownership of this attempt is already established (by [[claimKarmaCoinLookup]] or
   * [[transitionKarmaCoinLookup]] above): writing the plan, finalizing SUCCESS, or the FAILED path
   * for a genuinely fresh claim. No LWT needed here - the earlier conditional write is what
   * guarantees exclusivity; this just records the next state.
   */
  def updateKarmaCoinLookup(userKarmaCoinKey: String, operationType: String, creditDate: Long, addInfo: String)
                           (implicit metrics: Metrics): Unit = {
    val applied = guard("updateKarmaCoinLookup") {
      val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_coin_lookup_table)
        .value(config.DB_COLUMN_USER_KARMA_COIN_KEY, userKarmaCoinKey)
        .value(config.DB_COLUMN_OPERATION_TYPE, operationType)
        .value(config.DB_COLUMN_CREDIT_DATE, creditDate)
        .value(config.ADD_INFO, addInfo)
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.update(query)
    }
    if (!applied) {
      throw CassandraException(s"Database write was not applied for user_karma_coin_lookup key=$userKarmaCoinKey, operationType=$operationType")
    }
    metrics.incCounter(config.dbUpdateCount)
  }

  /** Upserts `total_earned`/`total_redeemed` for the user's Karma Coin wallet - an absolute set to
   * the caller's computed target values (see [[org.sunbird.job.karmapoints.v2.handlers.PointsConversionHandler]]'s
   * "frozen plan" design), so re-running this with the same arguments on a replay is a no-op, not
   * a double-count. LOCAL_QUORUM. */
  def updateKarmaCoinWallet(userId: String, totalEarned: Int, totalRedeemed: Int)(implicit metrics: Metrics): Unit = {
    val applied = guard("updateKarmaCoinWallet") {
      val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_coin_wallet_table)
        .value(config.DB_COLUMN_USERID, userId)
        .value(config.TOTAL_EARNED, totalEarned)
        .value(config.TOTAL_REDEEMED, totalRedeemed)
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.update(query)
    }
    if (!applied) {
      throw CassandraException(s"Database write was not applied for user_karma_coin_wallet userId=$userId")
    }
    metrics.incCounter(config.dbUpdateCount)
  }

  /** Upserts `points_converted` for the user's current-month Karma Coin conversion summary - same
   * absolute-set-from-frozen-plan idempotency as [[updateKarmaCoinWallet]]. LOCAL_QUORUM. */
  def updateKarmaCoinMonthlySummary(userId: String, yearMonth: String, pointsConverted: Int, updatedOn: Long)
                                   (implicit metrics: Metrics): Unit = {
    val applied = guard("updateKarmaCoinMonthlySummary") {
      val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_coin_monthly_summary_table)
        .value(config.DB_COLUMN_USERID, userId)
        .value(config.YEAR_MONTH, yearMonth)
        .value(config.POINTS_CONVERTED, pointsConverted)
        .value(config.UPDATED_ON, updatedOn)
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.update(query)
    }
    if (!applied) {
      throw CassandraException(s"Database write was not applied for user_karma_coin_monthly_summary userId=$userId, yearMonth=$yearMonth")
    }
    metrics.incCounter(config.dbUpdateCount)
  }

  /** Inserts one row into the Karma Coin transaction ledger. `(userid, created_at, transaction_id)`
   * is the primary key and, per the frozen-plan design, identical on every replay for the same
   * logical request - re-inserting identical column values is a harmless no-op. LOCAL_QUORUM. */
  def insertKarmaCoinTransaction(userId: String, createdAt: Long, transactionId: String, transactionType: String, amount: Long,
                                 balanceAfter: Int, actionType: String, contextType: String, contextId: String, addInfo: String)
                                (implicit metrics: Metrics): Unit = {
    val applied = guard("insertKarmaCoinTransaction") {
      val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_coin_transactions_table)
        .value(config.DB_COLUMN_USERID, userId)
        .value(config.CREATED_AT, createdAt)
        .value(config.DB_COLUMN_TRANSACTION_ID, transactionId)
        .value(config.TYPE, transactionType)
        .value(config.AMOUNT, amount)
        .value(config.BALANCE_AFTER, balanceAfter)
        .value(config.ACTION_TYPE, actionType)
        .value(config.CONTEXT_TYPE, contextType)
        .value(config.CONTEXT_ID, contextId)
        .value(config.ADD_INFO, addInfo)
      query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.update(query)
    }
    if (!applied) {
      throw CassandraException(s"Database insert was not applied for user_karma_coin_transactions userId=$userId, transactionId=$transactionId")
    }
    metrics.incCounter(config.dbUpdateCount)
  }

  // ---- Karma Points writes ----

  private def updatePoints(userId: String, contextType: String, operationType: String, contextId: String,
                           points: Int, addInfo: String, creditDate: Long): Boolean = guard("updatePoints") {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_table)
      .value(config.USER_ID, userId)
      .value(config.CREDIT_DATE, creditDate)
      .value(config.CONTEXT_TYPE, contextType)
      .value(config.OPERATION_TYPE, operationType)
      .value(config.CONTEXT_ID, contextId)
      .value(config.ADD_INFO, addInfo)
      .value(config.POINTS, points)
    cassandraUtil.upsert(query.toString)
  }

  private def insertKarmaCreditLookup(userId: String, contextType: String, operationType: String,
                                      contextId: String, creditDate: Long): Boolean = guard("insertKarmaCreditLookup") {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
      .value(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId)
      .value(config.DB_COLUMN_OPERATION_TYPE, operationType)
      .value(config.DB_COLUMN_CREDIT_DATE, creditDate)
    cassandraUtil.upsert(query.toString)
  }

  /** Insert a brand-new karma-points credit row (new courses/first-time credits). */
  def insertKarmaPoints(userId: String, contextType: String, operationType: String, contextId: String,
                        points: Int, addInfo: String, creditDate: Long = System.currentTimeMillis())
                       (implicit metrics: Metrics): Unit = {
    val applied = updatePoints(userId, contextType, operationType, contextId, points, addInfo, creditDate)
    if (!applied) {
      throw CassandraException(s"Database insert was not applied for userId=$userId, operationType=$operationType, contextId=$contextId")
    }
    insertKarmaCreditLookup(userId, contextType, operationType, contextId, creditDate)
    metrics.incCounter(config.dbUpdateCount)
  }

  /** Update an existing karma-points row in place (same credit_date) - used for re-enrolment / ACBP top-up. */
  def updateExistingKarmaPoints(userId: String, contextType: String, operationType: String, contextId: String,
                                points: Int, addInfo: String, creditDate: Long)(implicit metrics: Metrics): Unit = {
    val applied = updatePoints(userId, contextType, operationType, contextId, points, addInfo, creditDate)
    if (!applied) {
      throw CassandraException(s"Database update was not applied for userId=$userId, operationType=$operationType, contextId=$contextId")
    }
    metrics.incCounter(config.dbUpdateCount)
  }

  /** Writes the new summary total to Cassandra and returns it. Redis mirroring is the caller's job (RedisUtil). */
  def updateUserKarmaPointsSummary(userId: String, points: Int, addInfo: String): Int = guard("updateUserKarmaPointsSummary") {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_summary_table)
      .value(config.USER_ID, userId)
      .value(config.TOTAL_POINTS, points)
    if (addInfo != null) query.value(config.ADD_INFO, addInfo)
    cassandraUtil.upsert(query.toString)
    points
  }

  /** Adds `points` to the user's running total. Returns the new total (for Redis mirroring). */
  def addToKarmaSummary(userId: String, points: Int): Int = {
    val (total, _) = readSummary(userId)
    updateUserKarmaPointsSummary(userId, total + points, null)
  }

  /**
   * Same as [[addToKarmaSummary]] but also tracks the monthly non-ACBP course quota counter used to
   * cap non-ACBP course-completion awards (V1 `Utility.processUserKarmaSummaryUpdate`).
   *
   * @param nonACBPQuota +1 to consume a slot, -1 to free one (ACBP claim on an already-completed course).
   * @return the new running total.
   */
  def applyKarmaSummaryUpdate(userId: String, points: Int, nonACBPQuota: Int): Int = {
    val (total, infoMap) = readSummary(userId)
    val currentDateStr = LocalDate.now.format(DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM))
    var quotaCount = 0
    val currStr = infoMap.get(config.FORMATTED_MONTH)
    if (currStr != null && currentDateStr.equals(currStr)) {
      quotaCount = infoMap.getOrDefault(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, Integer.valueOf(0)).asInstanceOf[Int]
    }
    quotaCount = Math.max(0, quotaCount + nonACBPQuota)
    infoMap.put(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, quotaCount)
    infoMap.put(config.FORMATTED_MONTH, currentDateStr)
    val info = mapper.writeValueAsString(infoMap)
    updateUserKarmaPointsSummary(userId, total + points, info)
  }

  /**
   * Zeroes out an existing entry in place (unenrolment reversal). Returns the amount reverted
   * (as a negative delta the caller should apply to the summary/Redis total), or 0 if nothing to revert.
   */
  def revertKarmaPoints(userId: String, contextType: String, operationType: String, contextId: String,
                        addInfoRevertFlagKey: String)(implicit metrics: Metrics): Int = {
    val lookup = fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, contextId)
    if (lookup == null || lookup.isEmpty) return 0
    val creditDate = lookup.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
    val entry = fetchUserKarmaPoints(creditDate, userId, contextType, operationType, contextId)
    if (entry == null || entry.isEmpty) return 0
    val currentPoints = entry.get(0).getInt(config.POINTS)
    if (currentPoints == 0) return 0
    val addInfo = buildAddInfo(entry.get(0).getString(config.ADD_INFO), addInfoRevertFlagKey -> java.lang.Boolean.TRUE)
    updateExistingKarmaPoints(userId, contextType, operationType, contextId, 0, addInfo, creditDate.getTime)
    metrics.incCounter(config.dbUpdateCount)
    -currentPoints
  }
}
