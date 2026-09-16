package org.sunbird.job.karmapoints.v2.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.functions.KarmaPointsProcessorFnV2
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File

/**
 * V2 replacement for V1's `KarmaPointsProcessorTask`: one Kafka source (`karma-points-unified-v2`)
 * instead of 7, keyed by userId so per-user events land on the same subtask in order, feeding a
 * single [[KarmaPointsProcessorFnV2]] instead of 7 separate `*ProcessorFn` pipelines.
 */
class KarmaPointsProcessorTaskV2(config: KarmaPointsV2Config, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsProcessorTaskV2])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    // FlinkUtil hardcodes AT_LEAST_ONCE for every job in this repo; V2's requirement is
    // EXACTLY_ONCE, set explicitly here rather than changing the shared jobs-core default.
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    implicit val eventTypeInfo: TypeInformation[UnifiedEvent] = TypeExtractor.getForClass(classOf[UnifiedEvent])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[UnifiedEvent](config.kafkaInputTopic))
      .name(config.karmaPointsV2Consumer)
      .uid(config.karmaPointsV2Consumer)
      .setParallelism(config.kafkaConsumerParallelism)
      // Keyed per event type so per-user ordering still holds for all of them; see
      // KarmaPointsKeySelector for the per-event-type userId field paths.
      .keyBy(new KarmaPointsKeySelector(config))
      .process(new KarmaPointsProcessorFnV2(config, httpUtil))
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

object KarmaPointsProcessorTaskV2 {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("Karma-points-processorV2.conf").withFallback(ConfigFactory.systemEnvironment()))
    val karmaPointsV2Config = new KarmaPointsV2Config(config)
    val kafkaUtil = new FlinkKafkaConnector(karmaPointsV2Config)
    val httpUtil = new HttpUtil()
    val task = new KarmaPointsProcessorTaskV2(karmaPointsV2Config, kafkaUtil, httpUtil)
    task.process()
  }
}

/**
 * RATING's and EVENT_ATTENDED's V1 payloads carry userId at data.user_id,
 * FIRST_ENROLMENT's and ACBP_CLAIM's at data.edata.userId, FIRST_LOGIN's at data.edata.id,
 * UNENROLMENT's at data.edata.userIds, COURSE_COMPLETION's at edata.userIds[0] (unwrapped,
 * a JSON array) - none of them have the top-level `userId` field every other event type uses.
 *
 * Defined as a standalone KeySelector (rather than an inline lambda in `keyBy`) because a lambda
 * referencing `config` would capture the enclosing KarmaPointsProcessorTaskV2 instance, which is
 * not Serializable, causing a Flink "Task not serializable" failure at job submission.
 */
class KarmaPointsKeySelector(config: KarmaPointsV2Config) extends KeySelector[UnifiedEvent, String] {
  override def getKey(event: UnifiedEvent): String = event.eventType match {
    case config.EVENT_TYPE_RATING | config.EVENT_TYPE_EVENT_ATTENDED => event.dataString("user_id")
    case config.EVENT_TYPE_FIRST_ENROLMENT | config.EVENT_TYPE_ACBP_CLAIM => event.dataEdataString("userId")
    case config.EVENT_TYPE_FIRST_LOGIN => event.dataEdataString(config.ID)
    case config.EVENT_TYPE_UNENROLMENT => event.dataEdataString("userIds")
    case config.EVENT_TYPE_COURSE_COMPLETION => event.edataStringArrayFirst("userIds")
    case config.EVENT_TYPE_POINTS_CONVERSION | config.EVENT_TYPE_COINS_REDEMPTION | config.EVENT_TYPE_COINS_REAWARD =>
      event.dataString("userId")
    case _ => event.userId
  }
}
