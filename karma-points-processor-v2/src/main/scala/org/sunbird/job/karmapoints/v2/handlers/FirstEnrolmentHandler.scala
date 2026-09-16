package org.sunbird.job.karmapoints.v2.handlers

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

import java.util.Date

/**
 * Ports V1 `FirstEnrolmentProcessorFn`: awards `firstEnrolmentQuotaKarmaPoints` once ever across
 * all courses. If the user has no credit-lookup row for this course, award only if they've never
 * actively earned first-enrolment points anywhere (covers genuinely-new users and users whose
 * only prior first-enrolment entry was reverted to 0 by an unenrolment). If a row for this exact
 * course already has points > 0 it's a duplicate/redelivered event - skip. If it's a reverted (0)
 * row, re-enrolling re-awards on the same row, tagged as a re-enrolment.
 */
class FirstEnrolmentHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[FirstEnrolmentHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    val courseId = event.dataEdataString("courseId")
    if (courseId.isEmpty) {
      throw MissingPayloadException(s"data.edata.courseId is required for FIRST_ENROLMENT event, userId=$userId")
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing FIRST_ENROLMENT event: userId=$userId, courseId=$courseId")

    val hierarchy = cassandraUtil.fetchContentHierarchy(courseId)
    if (hierarchy == null || hierarchy.isEmpty) {
      logger.info(s"No content hierarchy for courseId=$courseId - skipping first-enrolment, userId=$userId")
      metrics.incCounter(config.skippedEventCount)
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    if (!config.COURSE.equalsIgnoreCase(contextType)) {
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.firstEnrolmentQuotaKarmaPoints
    val operationType = config.OPERATION_TYPE_ENROLMENT
    val lookup = cassandraUtil.fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, courseId)

    val awarded = if (lookup == null || lookup.isEmpty) {
      if (cassandraUtil.hasEarnedFirstEnrolmentPoints(userId)) {
        false
      } else {
        val addInfo = cassandraUtil.buildAddInfo(null, config.ADDINFO_COURSENAME -> hierarchy.get(config.name))
        cassandraUtil.insertKarmaPoints(userId, contextType, operationType, courseId, points, addInfo)
        true
      }
    } else {
      val creditDate = lookup.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
      val entry = cassandraUtil.fetchUserKarmaPoints(creditDate, userId, contextType, operationType, courseId)
      if (entry == null || entry.isEmpty || entry.get(0).getInt(config.POINTS) > 0) {
        false
      } else if (cassandraUtil.hasEarnedFirstEnrolmentPoints(userId)) {
        false
      } else {
        val addInfo = cassandraUtil.buildAddInfo(entry.get(0).getString(config.ADD_INFO),
          config.ADDINFO_COURSENAME -> hierarchy.get(config.name), config.ADDINFO_REENROLMENT -> java.lang.Boolean.TRUE)
        cassandraUtil.updateExistingKarmaPoints(userId, contextType, operationType, courseId, points, addInfo, creditDate.getTime)
        true
      }
    }

    if (awarded) {
      // TODO: Remove temporary INFO log after testing.
      logger.info(
        s"FIRST_ENROLMENT points awarded: userId=$userId, courseId=$courseId, points=$points"
      )
      val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
      redisUtil.setUserKarmaPoints(userId, newTotal)
    } else {
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
