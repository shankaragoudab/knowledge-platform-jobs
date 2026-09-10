package org.sunbird.job.karmapoints.v2.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.util.JSONUtil

/**
 * Publishes data-quality failures to `karma-points-unified-v2-failed` so the job can keep running
 * (offset commits, no restart) while the bad event is preserved for triage/replay tooling.
 * Constructed once per subtask in `open()`, so it owns its own KafkaProducer rather than reusing
 * the consumer's client.
 */
class FailedEventProducer(config: KarmaPointsV2Config) extends Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[FailedEventProducer])
  @transient private var producer: KafkaProducer[String, String] = _

  def init(): Unit = {
    val props = config.kafkaProducerProperties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](props)
  }

  def send(event: UnifiedEvent, errorType: String, errorMessage: String): Unit = {
    try {
      val failurePayload = new java.util.HashMap[String, Any]()
      failurePayload.put("originalEvent", event.getMap())
      failurePayload.put("userId", event.userId)
      failurePayload.put("eventType", event.eventType)
      failurePayload.put("errorType", errorType)
      failurePayload.put("errorMessage", errorMessage)
      failurePayload.put("failedAt", System.currentTimeMillis())
      val json = JSONUtil.serialize(failurePayload)
      val key = if (event.userId != null && event.userId.nonEmpty) event.userId else "unknown"
      producer.send(new ProducerRecord[String, String](config.kafkaFailedTopic, key, json))
    } catch {
      case ex: Exception =>
        // A failed-topic publish failure must not itself crash the job (that would defeat the
        // purpose of the 2-path split) - log loudly for alerting and move on.
        logger.error(s"Failed to publish failed-event to ${config.kafkaFailedTopic} for userId=${event.userId}, eventType=${event.eventType}", ex)
    }
  }

  def close(): Unit = {
    try {
      if (producer != null) producer.close()
    } catch {
      case ex: Exception => logger.warn("Error closing FailedEventProducer", ex)
    }
  }
}
