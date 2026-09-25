package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Handles ENGAGEMENT_STREAK events:
 * {
 *   "eventType": "ENGAGEMENT_STREAK",
 *   "data": { "edata": { "userId": "user123", "startDate": "04-09-2026", "endDate": "25-09-2026" } },
 *   "version": 1
 * }
 * Kafka key: userId
 *
 * One-time-per-user karma-points award (config.engagementStreakQuotaKarmaPoints). Dedup identity is
 * the plain key `userId` / `operation_type=ENGAGEMENT_STREAK` in `user_karma_points_credit_lookup` -
 * same shape as SelfRegistrationHandler/VerifiedProfileHandler, so this reuses the generic
 * `CassandraUtil.doesEntryExistByKey(userKarmaPointsKey, operationType)` for the check and
 * `CassandraUtil.insertKarmaPointsWithLookupKey(...)` for the write. The ledger row's
 * `context_type`/`context_id` are `ENGAGEMENT_STREAK`/`userId` per the event spec, with
 * `startDate`/`endDate` carried only in `addinfo`. `addToKarmaSummary`/`RedisUtil.setUserKarmaPoints`
 * are reused unchanged, same as every other one-time-award handler in this package.
 */
class EngagementStreakHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[EngagementStreakHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    val startDate = event.dataEdataString("startDate")
    val endDate = event.dataEdataString("endDate")
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing ENGAGEMENT_STREAK event: userId=$userId, startDate=$startDate, endDate=$endDate")

    if (StringUtils.isBlank(userId)) {
      throw MissingPayloadException("ENGAGEMENT_STREAK event is missing userId (data.edata.userId)")
    }
    if (StringUtils.isBlank(startDate)) {
      throw MissingPayloadException(s"ENGAGEMENT_STREAK event is missing startDate (data.edata.startDate) for userId=$userId")
    }
    if (StringUtils.isBlank(endDate)) {
      throw MissingPayloadException(s"ENGAGEMENT_STREAK event is missing endDate (data.edata.endDate) for userId=$userId")
    }

    if (cassandraUtil.doesEntryExistByKey(userId, config.OPERATION_TYPE_ENGAGEMENT_STREAK)) {
      logger.info(s"ENGAGEMENT_STREAK karma points already awarded for userId=$userId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.engagementStreakQuotaKarmaPoints
    val addInfo = cassandraUtil.buildAddInfo(null, config.ADDINFO_START_DATE -> startDate, config.ADDINFO_END_DATE -> endDate)
    cassandraUtil.insertKarmaPointsWithLookupKey(userId, config.OPERATION_TYPE_ENGAGEMENT_STREAK,
      config.OPERATION_TYPE_ENGAGEMENT_STREAK, userId, points, addInfo, userId)
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"ENGAGEMENT_STREAK points awarded: userId=$userId, points=$points")
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
