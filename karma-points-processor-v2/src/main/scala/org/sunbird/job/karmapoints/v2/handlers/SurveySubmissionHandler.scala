package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

/**
 * Handles SURVEY_SUBMISSION events:
 * {
 * "eventType": "SURVEY_SUBMISSION",
 * "data": { "edata": { "userId": "user123", "courseId": "course123", "surveyId": "form123" } },
 * "version": 1
 * }
 * Kafka key: userId
 *
 * Once-per-user-per-course-per-survey karma-points award (config.surveySubmissionQuotaKarmaPoints).
 * Dedup identity is `userId|courseId|surveyId` / `operation_type=SURVEY_SUBMISSION` in
 * `user_karma_points_credit_lookup` - an independent lookup key, NOT the `userId|contextType|contextId`
 * composite `CassandraUtil.insertKarmaPoints` builds - so this reuses
 * `CassandraUtil.doesEntryExistByKey(lookupKey, operationType)` and
 * `CassandraUtil.insertKarmaPointsWithLookupKey(userId, contextType, operationType, contextId,
 * points, addInfo, lookupKey)` instead, with ledger `context_type=config.COURSE`/`context_id=courseId`
 * and `addinfo={"surveyId":...}` via the existing `buildAddInfo`.
 *
 * No monthly quota is enforced - none exists in this codebase for SURVEY_SUBMISSION (the only
 * monthly counter, `nonAcbpCourseQuota`, is hard-wired to non-ACBP course completion and reusing it
 * would require changing its behavior) - confirmed with the requester; same one-time-per-key shape
 * as FirstLoginHandler/VerifiedProfileHandler/SelfRegistrationHandler.
 */
class SurveySubmissionHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {
  private[this] val logger = LoggerFactory.getLogger(classOf[SurveySubmissionHandler])

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    val courseId = event.dataEdataString("courseId")
    val surveyId = event.dataEdataString("surveyId")
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing SURVEY_SUBMISSION event: userId=$userId, courseId=$courseId, surveyId=$surveyId")

    if (StringUtils.isBlank(userId)) {
      throw MissingPayloadException("SURVEY_SUBMISSION event is missing userId (data.edata.userId)")
    }
    if (StringUtils.isBlank(courseId)) {
      throw MissingPayloadException(s"SURVEY_SUBMISSION event is missing courseId (data.edata.courseId) for userId=$userId")
    }
    if (StringUtils.isBlank(surveyId)) {
      throw MissingPayloadException(s"SURVEY_SUBMISSION event is missing surveyId (data.edata.surveyId) for userId=$userId")
    }

    val lookupKey = s"$userId${config.PIPE}$courseId${config.PIPE}$surveyId"
    if (cassandraUtil.doesEntryExistByKey(lookupKey, config.OPERATION_TYPE_SURVEY_SUBMISSION)) {
      logger.info(s"SURVEY_SUBMISSION karma points already awarded for userId=$userId, courseId=$courseId, surveyId=$surveyId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.surveySubmissionQuotaKarmaPoints
    val addInfo = cassandraUtil.buildAddInfo(null, config.ADDINFO_SURVEY_ID -> surveyId)
    cassandraUtil.insertKarmaPointsWithLookupKey(userId, config.COURSE, config.OPERATION_TYPE_SURVEY_SUBMISSION,
      courseId, points, addInfo, lookupKey)
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"SURVEY_SUBMISSION points awarded: userId=$userId, courseId=$courseId, surveyId=$surveyId, points=$points")
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
