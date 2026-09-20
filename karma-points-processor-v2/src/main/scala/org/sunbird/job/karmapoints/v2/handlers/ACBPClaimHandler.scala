package org.sunbird.job.karmapoints.v2.handlers

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.karmapoints.v2.utils.ExternalServiceClient
import org.sunbird.job.util.JSONUtil

import java.util
import java.util.Date

/**
 * Ports V1 `ClaimACBPProcessorFn`: a retroactive ACBP-plan claim on an already-completed course.
 * If the course has no COURSE_COMPLETION credit-lookup row yet, insert a new one flagged as ACBP
 * and free one non-ACBP monthly quota slot. If it already has a row, top it up with the ACBP
 * bonus once (idempotent via the ADDINFO_ACBP flag already set) and also free a quota slot.
 */
class ACBPClaimHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil,
                       externalServiceClient: ExternalServiceClient) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[ACBPClaimHandler])
  private lazy val mapper: ObjectMapper = new ObjectMapper()

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataEdataString("userId")
    val courseId = event.dataEdataString("courseId")
    if (courseId.isEmpty) {
      throw MissingPayloadException(s"data.edata.courseId is required for ACBP_CLAIM event, userId=$userId")
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing ACBP_CLAIM event: userId=$userId, courseId=$courseId")

    val headers = Map(
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON,
      config.X_AUTHENTICATED_USER_ORGID -> cassandraUtil.fetchUserRootOrgId(userId),
      config.X_AUTHENTICATED_USER_ID -> userId
    )
    if (!externalServiceClient.isCourseOnACBPPlan(courseId, headers)) {
      logger.info(s"courseId=$courseId is not part of an ACBP plan for userId=$userId - skipping claim")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val hierarchy = cassandraUtil.fetchContentHierarchy(courseId)
    if (hierarchy == null || hierarchy.isEmpty) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    if (config.COURSE != contextType) {
      metrics.incCounter(config.skippedEventCount)
      return
    }
    val courseName = hierarchy.get(config.name).asInstanceOf[String]
    val operationType = config.OPERATION_COURSE_COMPLETION

    val lookup = cassandraUtil.fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, courseId)
    val awardedPoints = if (lookup == null || lookup.isEmpty) {
      logger.info(s"Making new ACBP entry for userId=$userId, courseId=$courseId")
      val addInfoMap = new util.HashMap[String, AnyRef]()
      addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
      addInfoMap.put(config.OPERATION_COURSE_COMPLETION, java.lang.Boolean.FALSE)
      addInfoMap.put(config.ADDINFO_COURSENAME, courseName)
      val addInfo = mapper.writeValueAsString(addInfoMap)
      cassandraUtil.insertKarmaPoints(userId, contextType, operationType, courseId, config.acbpQuotaKarmaPoints, addInfo)
      Some(config.acbpQuotaKarmaPoints)
    } else {
      val creditDate = lookup.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
      val entry = cassandraUtil.fetchUserKarmaPoints(creditDate, userId, contextType, operationType, courseId)
      val currentPoints = entry.get(0).getInt(config.POINTS)
      val addInfo = entry.get(0).getString(config.ADD_INFO)
      val addInfoMap = JSONUtil.deserialize[java.util.HashMap[String, Any]](addInfo)
      if (addInfoMap.getOrDefault(config.ADDINFO_ACBP, java.lang.Boolean.FALSE).asInstanceOf[Boolean]) {
        logger.info(s"ACBP bonus already applied for userId=$userId, courseId=$courseId - skipping duplicate claim")
        None
      } else {
        logger.info(s"Updating existing entry with ACBP bonus for userId=$userId, courseId=$courseId")
        addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
        val addInfoStr = mapper.writeValueAsString(addInfoMap)
        val newPoints = currentPoints + config.acbpQuotaKarmaPoints
        cassandraUtil.updateExistingKarmaPoints(userId, contextType, operationType, courseId, newPoints, addInfoStr, creditDate.getTime)
        Some(config.acbpQuotaKarmaPoints)
      }
    }

    awardedPoints match {
      case Some(points) =>
        // TODO: Remove temporary INFO log after testing.
        logger.info(
          s"ACBP_CLAIM points awarded: userId=$userId, courseId=$courseId, points=$points"
        )
        // Frees one non-ACBP monthly quota slot (nonACBPQuota = -1), same accounting as V1.
        val newTotal = cassandraUtil.applyKarmaSummaryUpdate(userId, points, -1)
        redisUtil.setUserKarmaPoints(userId, newTotal)
      case None =>
        metrics.incCounter(config.skippedEventCount)
    }
  }
}
