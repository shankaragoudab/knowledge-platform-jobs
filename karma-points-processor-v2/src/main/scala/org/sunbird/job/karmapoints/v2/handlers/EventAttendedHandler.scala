package org.sunbird.job.karmapoints.v2.handlers

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.MissingPayloadException
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.karmapoints.v2.utils.ExternalServiceClient

import java.util

/**
 * Ports V1 `EventAttendedProcessorFn`: awards `eventQuotaKarmaPoints` for attending an event,
 * deduped via the existing (CONTEXT_TYPE_EVENT, OPERATION_TYPE_EVENT, eventId) credit-lookup key,
 * and only if the event was consumed (`edata.ets`) before the event's `endDate`/`endTime` as read
 * from the event-service API - identical gating to V1.
 */
class EventAttendedHandler(config: KarmaPointsV2Config, cassandraUtil: CassandraUtil, redisUtil: RedisUtil,
                           externalServiceClient: ExternalServiceClient) extends EventHandler {

  private[this] val logger = LoggerFactory.getLogger(classOf[EventAttendedHandler])
  private lazy val mapper: ObjectMapper = new ObjectMapper()

  override protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    val userId = event.dataString("user_id")
    val eventId = event.dataString("event_id")
    if (eventId.isEmpty) {
      throw MissingPayloadException(s"data.event_id is required for EVENT_ATTENDED event, userId=$userId")
    }
    // TODO: Remove temporary INFO log after testing.
    logger.info(s"Processing EVENT_ATTENDED event: userId=$userId, eventId=$eventId")
    val ets = event.dataLong("ets", event.ets)
    val etsDate = new java.util.Date(ets)

    if (cassandraUtil.doesEntryExist(userId, config.CONTEXT_TYPE_EVENT, config.OPERATION_TYPE_EVENT, eventId)) {
      logger.info(s"Karma points already awarded for eventId=$eventId, userId=$userId - skipping duplicate")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val headers = Map(config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON)
    val (eventName, endDate) = externalServiceClient.eventNameAndEndDate(eventId, headers)
    if (etsDate.after(endDate)) {
      logger.info(s"Karma points not allocated - event endDate=$endDate has passed, consumed at $etsDate, eventId=$eventId")
      metrics.incCounter(config.skippedEventCount)
      return
    }

    val points = config.eventQuotaKarmaPoints
    val addInfoMap = new util.HashMap[String, AnyRef]()
    addInfoMap.put(config.ADDINFO_EVENTNAME, eventName)
    val addInfo = mapper.writeValueAsString(addInfoMap)

    cassandraUtil.insertKarmaPoints(userId, config.CONTEXT_TYPE_EVENT, config.OPERATION_TYPE_EVENT, eventId, points, addInfo, etsDate.getTime)
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"EVENT_ATTENDED points awarded: userId=$userId, eventId=$eventId, points=$points"
    )
    val newTotal = cassandraUtil.addToKarmaSummary(userId, points)
    redisUtil.setUserKarmaPoints(userId, newTotal)
  }
}
