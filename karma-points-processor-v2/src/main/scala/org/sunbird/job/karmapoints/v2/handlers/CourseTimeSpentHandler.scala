package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Handles COURSE_TIME_SPENT events:
 * {
 * "eventType": "COURSE_TIME_SPENT",
 * "data": { "edata": { "userId": "user123", "courseId": "course123", "batchId": "batch123" } },
 * "version": 1
 * }
 * Kafka key: userId
 *
 * Once-per-user-per-course karma-points award (config.courseTimeSpentQuotaKarmaPoints). Same shape
 * as SurveySubmissionHandler: dedup identity is `userId|COURSE_TIME_SPENT|courseId` /
 * `operation_type=COURSE_TIME_SPENT` in `user_karma_points_credit_lookup` - exactly the composite
 * key `CassandraUtil.doesEntryExist`/`insertKarmaPoints` already build from
 * `contextType=operationType="COURSE_TIME_SPENT"`, `contextId=courseId` - both reused completely
 * unmodified. `batchId` is required by the event contract but is NOT part of the dedup key (per
 * spec); it's carried into the ledger row's `addinfo` via the existing `buildAddInfo` convention
 * (same pattern as FirstEnrolmentHandler's ADDINFO_COURSENAME).
 *
 * No monthly quota is enforced - none exists in this codebase for COURSE_TIME_SPENT, same gap as
 * SURVEY_SUBMISSION; confirmed with the requester to proceed without one.
 */
class CourseTimeSpentHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[CourseTimeSpentHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    val courseId = event.dataEdataString("courseId")
    val batchId = event.dataEdataString("batchId")
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing COURSE_TIME_SPENT event: userId=$userId, courseId=$courseId, batchId=$batchId")

    if (StringUtils.isBlank(userId)) {
      throw MissingPayloadException("COURSE_TIME_SPENT event is missing userId (data.edata.userId)")
    }
    if (StringUtils.isBlank(courseId)) {
      throw MissingPayloadException(s"COURSE_TIME_SPENT event is missing courseId (data.edata.courseId) for userId=$userId")
    }
    if (StringUtils.isBlank(batchId)) {
      throw MissingPayloadException(s"COURSE_TIME_SPENT event is missing batchId (data.edata.batchId) for userId=$userId")
    }

    if (cassandraUtil.doesEntryExist(userId, config.OPERATION_TYPE_COURSE_TIME_SPENT, config.OPERATION_TYPE_COURSE_TIME_SPENT, courseId)) {
      logger.info(s"COURSE_TIME_SPENT karma points already awarded for userId=$userId, courseId=$courseId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.courseTimeSpentQuotaKarmaPoints
    val addInfo = cassandraUtil.buildAddInfo(null, config.ADDINFO_BATCH_ID -> batchId)
    cassandraUtil.insertKarmaPoints(userId, config.OPERATION_TYPE_COURSE_TIME_SPENT, config.OPERATION_TYPE_COURSE_TIME_SPENT,
      courseId, points, addInfo)
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"COURSE_TIME_SPENT points awarded: userId=$userId, courseId=$courseId, batchId=$batchId, points=$points")
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
