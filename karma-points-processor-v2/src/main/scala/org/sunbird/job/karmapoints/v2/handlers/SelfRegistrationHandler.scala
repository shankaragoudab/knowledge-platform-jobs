package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.InvalidUserIdException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Handles SELF_REGISTRATION_KARMA_POINT events:
 * {
 *   "eventType": "SELF_REGISTRATION_KARMA_POINT",
 *   "data": { "edata": { "userId": "user123" } },
 *   "version": 1
 * }
 * Kafka key: userId
 *
 * One-time-per-user karma-points award (config.selfRegistrationQuotaKarmaPoints). Dedup identity
 * is the plain-key/operation_type pair `(userId, 'SELF_REGISTRATION')` in
 * `user_karma_points_credit_lookup` - NOT the `userId|contextType|contextId` composite key every
 * other operationType in that table uses, so this reuses
 * `CassandraUtil.doesVerifiedProfileEntryExist(userId, operationType)` (already generic on
 * `operationType` despite its name - first introduced for VERIFIED_PROFILE, unmodified here) for
 * the check, and `CassandraUtil.insertSelfRegistrationPoints` for the write (a small dedicated
 * method, since the existing composite-key `insertKarmaPoints` cannot produce a plain-`userId`
 * lookup key without changing its own behavior). `addToKarmaSummary`/`RedisUtil.setUserKarmaPoints`
 * are reused unchanged, same as every other one-time-award handler in this package.
 */
class SelfRegistrationHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[SelfRegistrationHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing SELF_REGISTRATION_KARMA_POINT event: userId=$userId")

    if (StringUtils.isBlank(userId)) {
      throw InvalidUserIdException("SELF_REGISTRATION_KARMA_POINT event is missing userId (data.edata.userId)")
    }

    if (cassandraUtil.doesVerifiedProfileEntryExist(userId, config.OPERATION_TYPE_SELF_REGISTRATION)) {
      logger.info(s"SELF_REGISTRATION karma points already awarded for userId=$userId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.selfRegistrationQuotaKarmaPoints
    cassandraUtil.insertSelfRegistrationPoints(userId, points)
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"SELF_REGISTRATION points awarded: userId=$userId, points=$points")
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
