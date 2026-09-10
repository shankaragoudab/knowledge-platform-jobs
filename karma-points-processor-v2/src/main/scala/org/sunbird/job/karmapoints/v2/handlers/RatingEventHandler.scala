package org.sunbird.job.karmapoints.v2.handlers

import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}

import java.util

/**
 * Ports V1 `RatingProcessorFn`: awards a fixed `ratingQuotaKarmaPoints` (2) once per
 * (userId, contextType, activityId), deduped via the credit-lookup table exactly as in V1.
 * V2 producers wrap the original, unchanged V1 rating payload under `data`
 * (`{eventType: "RATING", data: {..V1 payload as-is.., user_id, activity_id, ...}}`), so this
 * handler reads `user_id`/`activity_id` from `event.data` using V1's own field names/casing -
 * no renaming, no restructuring.
 */
class RatingEventHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[RatingEventHandler])
  private lazy val mapper: ObjectMapper = new ObjectMapper()

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataString("user_id")
    val activityId = event.dataString("activity_id")
    if (StringUtils.isEmpty(activityId)) {
      throw MissingPayloadException(s"data.activity_id is required for RATING event, userId=$userId")
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing RATING event: userId=$userId, activityId=$activityId")

    val hierarchy = cassandraUtil.fetchContentHierarchy(activityId)
    if (hierarchy == null || hierarchy.isEmpty) {
      logger.info(s"No content hierarchy for activityId=$activityId - skipping rating event, userId=$userId")
      metrics.incCounter(config.skippedEventCount)
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    val courseName = hierarchy.get(config.name).asInstanceOf[String]

    if (cassandraUtil.doesEntryExist(userId, contextType, config.OPERATION_TYPE_RATING, activityId)) {
      logger.info(s"Rating karma points already awarded for userId=$userId, activityId=$activityId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"RATING validation passed, awarding points: userId=$userId, activityId=$activityId")


    val points = config.ratingQuotaKarmaPoints
    val addInfoMap = new util.HashMap[String, AnyRef]()
    addInfoMap.put(config.ADDINFO_COURSENAME, courseName)
    val addInfo = mapper.writeValueAsString(addInfoMap)

    cassandraUtil.insertKarmaPoints(userId, contextType, config.OPERATION_TYPE_RATING, activityId, points, addInfo)
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
