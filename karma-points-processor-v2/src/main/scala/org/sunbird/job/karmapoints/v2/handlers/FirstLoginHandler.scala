package org.sunbird.job.karmapoints.v2.handlers

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Ports V1 `FirstLoginProcessorFn`: awards `firstLoginQuotaKarmaPoints` (5) once ever, only when
 * `edata.selfRegistration=true` and no prior FIRST_LOGIN credit-lookup entry exists for the user
 * (identical dedup key as V1: contextType=contextId=operationType=FIRST_LOGIN=userId row).
 */
class FirstLoginHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[FirstLoginHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("id")
    val selfRegistration = event.dataEdataBoolean("self_registration")
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"Processing FIRST_LOGIN event: userId=$userId, selfRegistration=$selfRegistration"
    )
    if (!selfRegistration) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    if (cassandraUtil.doesEntryExist(userId, config.OPERATION_TYPE_FIRST_LOGIN, config.OPERATION_TYPE_FIRST_LOGIN, userId)) {
      logger.info(s"FIRST_LOGIN karma points already awarded for userId=$userId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.firstLoginQuotaKarmaPoints
    cassandraUtil.insertKarmaPoints(userId, config.OPERATION_TYPE_FIRST_LOGIN, config.OPERATION_TYPE_FIRST_LOGIN, userId, points, config.EMPTY)
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"FIRST_LOGIN points awarded: userId=$userId, points=$points"
    )
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
