package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.InvalidUserIdException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Handles VERIFIED_PROFILE events:
 * {
 *   "eventType": "VERIFIED_PROFILE",
 *   "data": { "edata": { "userId": "user123" } },
 *   "version": 1
 * }
 *
 * Once-per-user karma-points award (config.verifiedProfileQuotaKarmaPoints). Dedup identity is the
 * plain-key/operation_type pair `(userId, 'VERIFIED_PROFILE')` in `user_karma_points_credit_lookup`,
 * checked via `CassandraUtil.doesEntryExistByKey` and written via
 * `CassandraUtil.insertKarmaPointsWithLookupKey` (ledger `context_type`/`context_id` = userId's
 * VERIFIED_PROFILE context, lookup key = plain userId, independent of the ledger fields).
 * `addToKarmaSummary`/`RedisUtil.setUserKarmaPoints` are reused unchanged, same as every other
 * one-time-award handler in this package.
 */
class VerifiedProfileHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[VerifiedProfileHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")

    if (StringUtils.isEmpty(userId)) {
      throw InvalidUserIdException("VERIFIED_PROFILE event is missing userId (data.edata.userId)")
    }

    if (cassandraUtil.doesEntryExistByKey(userId, config.OPERATION_TYPE_VERIFIED_PROFILE)) {
      logger.info(s"VERIFIED_PROFILE karma points already awarded for userId=$userId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.verifiedProfileQuotaKarmaPoints
    cassandraUtil.insertKarmaPointsWithLookupKey(userId, config.OPERATION_TYPE_VERIFIED_PROFILE,
      config.OPERATION_TYPE_VERIFIED_PROFILE, userId, points, config.EMPTY, userId)
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
