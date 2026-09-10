package org.sunbird.job.karmapoints.v2.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.karmapoints.v2.utils.ExternalServiceClient
import org.sunbird.job.util.JSONUtil

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, OffsetDateTime, Period}
import java.util

/**
 * Ports V1 `CourseCompletionProcessorFn` (the most involved of the 7 handlers): resolves the
 * multi-language variant of the course if one was completed, determines base points (course vs.
 * Learning Pathway completion), adds the assessment-pass bonus if the course has a Course
 * Assessment child and the user passed it, adds the ACBP bonus (decayed 1 point/month once past
 * the plan's expiry, floored at 0) if the course is on the user's ACBP plan, and gates non-ACBP
 * completions behind the monthly quota cap when capping is enabled. Point amounts and gating
 * logic are unchanged from V1 - only the storage/HTTP plumbing moved to the V2 storage/utils layers.
 */
class CourseCompletionHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil,
                              externalServiceClient: ExternalServiceClient) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[CourseCompletionHandler])
  private lazy val mapper: ObjectMapper = new ObjectMapper()

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.edataStringArrayFirst("userIds")
    if (userId.isEmpty) {
      throw MissingPayloadException(s"edata.userIds[0] is required for COURSE_COMPLETION event")
    }
    val courseId = event.edataString("courseId")
    if (courseId.isEmpty) {
      throw MissingPayloadException(s"edata.courseId is required for COURSE_COMPLETION event, userId=$userId")
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing COURSE_COMPLETION event: userId=$userId, courseId=$courseId")

    val action = event.edataString("action")
    if ("issue-event-certificate".equalsIgnoreCase(action)) {
      return
    }

    var hierarchy = cassandraUtil.fetchContentHierarchy(courseId)
    if (hierarchy == null || hierarchy.isEmpty) {
      logger.info(s"No content hierarchy for courseId=$courseId - skipping course-completion, userId=$userId")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val completedLang = event.edataString("completedLanguage")
    if (hierarchy.containsKey(config.LANGUAGE_MAP_v1) && completedLang.nonEmpty) {
      val languageMapV1 = hierarchy.get(config.LANGUAGE_MAP_v1).asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]]
      if (languageMapV1 != null && languageMapV1.containsKey(completedLang)) {
        val mlCourseId = languageMapV1.get(completedLang).get(config.ID).asInstanceOf[String]
        hierarchy = cassandraUtil.fetchContentHierarchy(mlCourseId)
      }
    }

    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    val courseCategory = hierarchy.getOrDefault(config.courseCategory, "").asInstanceOf[String]
    val operationType = if (config.LEARNING_PATHWAY.equals(courseCategory)) config.OPERATION_LEARNING_PATHWAY_COMPLETION else config.OPERATION_COURSE_COMPLETION

    val headers = Map(
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON,
      config.X_AUTHENTICATED_USER_ORGID -> cassandraUtil.fetchUserRootOrgId(userId),
      config.X_AUTHENTICATED_USER_ID -> userId
    )
    val acbpExpiry = externalServiceClient.acbpExpiryForCourse(courseId, headers)

    if (!passesValidation(contextType, courseId, userId, operationType, acbpExpiry)) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"COURSE_COMPLETION validation passed, awarding points: userId=$userId, " +
        s"courseId=$courseId, operationType=$operationType"
    )
    awardCourseCompletion(userId, contextType, courseCategory, operationType, courseId, hierarchy, acbpExpiry)
  }

  private def passesValidation(contextType: String, courseId: String, userId: String, operationType: String, acbpExpiry: String)
                              (implicit metrics: Metrics): Boolean = {
    if (!config.COURSE.equals(contextType)) return false
    if (config.enableKarmaPointsCapping && acbpExpiry.isEmpty && cassandraUtil.hasReachedNonACBPMonthlyCutOff(userId)) return false
    if (cassandraUtil.doesEntryExist(userId, contextType, operationType, courseId)) {
      logger.info(s"Karma points already awarded for userId=$userId, courseId=$courseId, operationType=$operationType - skipping duplicate")
      return false
    }
    true
  }

  private def awardCourseCompletion(userId: String, contextType: String, courseCategory: String, operationType: String,
                                    courseId: String, hierarchy: java.util.Map[String, AnyRef], acbpExpiry: String)
                                   (implicit metrics: Metrics): Unit = {
    var nonACBPCount = 1
    val addInfoMap = new util.HashMap[String, AnyRef]()
    addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.FALSE)
    addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.FALSE)
    addInfoMap.put(config.OPERATION_COURSE_COMPLETION, java.lang.Boolean.TRUE)
    addInfoMap.put(config.ADDINFO_COURSENAME, hierarchy.get(config.name))

    var points = if (config.LEARNING_PATHWAY.equals(courseCategory)) {
      logger.info(s"Awarding Learning Pathway completion points for userId=$userId, courseId=$courseId")
      config.learningPathwayCompletionQuotaKarmaPoints
    } else {
      config.courseCompletionQuotaKarmaPoints
    }

    val assessmentIdentifier = cassandraUtil.doesAssessmentExistInHierarchy(hierarchy)
    if (StringUtils.isNotEmpty(assessmentIdentifier)) {
      val assessmentResponse = cassandraUtil.fetchUserAssessmentResult(userId, assessmentIdentifier)
      var passed = false
      if (assessmentResponse != null && assessmentResponse.size() > 0) {
        val result = assessmentResponse.get(0).getString(config.DB_COLUMN_SUBMIT_ASSESSMENT_RESPONSE)
        if (StringUtils.isNotEmpty(result)) {
          val resultMap = JSONUtil.deserialize[java.util.Map[String, Any]](result)
          passed = resultMap.getOrDefault(config.PASS, java.lang.Boolean.FALSE).asInstanceOf[Boolean]
        }
      }
      addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.TRUE)
      addInfoMap.put(config.ADDINFO_ASSESSMENT_PASS, java.lang.Boolean.valueOf(passed))
      if (passed) points += config.assessmentQuotaKarmaPoints
    }

    if (StringUtils.isNotEmpty(acbpExpiry)) {
      nonACBPCount = 0
      points += config.acbpQuotaKarmaPoints
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
      val expiryDate = OffsetDateTime.parse(acbpExpiry, formatter).toLocalDateTime
      val now = LocalDateTime.now
      if (now.isAfter(expiryDate)) {
        val period = Period.between(expiryDate.toLocalDate, now.toLocalDate)
        val monthsLate = period.getYears * 12 + period.getMonths + 1
        points -= math.min(monthsLate, config.acbpQuotaKarmaPoints)
      }
      addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
    }

    val addInfo = try {
      mapper.writeValueAsString(addInfoMap)
    } catch {
      case e: JsonProcessingException => throw new RuntimeException(e)
    }

    cassandraUtil.insertKarmaPoints(userId, contextType, operationType, courseId, points, addInfo)
    val newTotal = cassandraUtil.applyKarmaSummaryUpdate(userId, points, nonACBPCount)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
