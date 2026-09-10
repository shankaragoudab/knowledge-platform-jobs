package org.sunbird.job.karmapoints.v2.handlers

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Ports V1 `UnenrolmentProcessorFn`: reverts (zeroes, doesn't delete) the user's FIRST_ENROLMENT
 * credit for the given course. A missing userId/courseId in V1 was a silent `return`, not a
 * failure - kept as-is here (skip, not routed to failed-topic) since a stray unenrolment event
 * with no course context genuinely has nothing to revert.
 */
class UnenrolmentHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[UnenrolmentHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userIds")
    val courseId = event.dataEdataString("courseId")
    if (userId.isEmpty || courseId.isEmpty) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"Processing UNENROLMENT event: userId=$userId, courseId=$courseId"
    )

    val hierarchy = cassandraUtil.fetchContentHierarchy(courseId)
    if (hierarchy == null || hierarchy.isEmpty) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]

    logger.info(s"Reverting first-enrolment karma points on unenrolment for userId=$userId, courseId=$courseId")
    val delta = cassandraUtil.revertKarmaPoints(userId, contextType, config.OPERATION_TYPE_ENROLMENT, courseId, config.ADDINFO_UNENROLMENT)
    if (delta != 0) {
      // TODO: Remove temporary INFO log after testing.
      logger.info(
        s"UNENROLMENT karma points reverted: userId=$userId, courseId=$courseId, delta=$delta"
      )
      val newTotal = cassandraUtil.addToKarmaSummary(userId, delta)
      redisUtil.setUserKarmaPoints(userId, newTotal)
    } else {
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
