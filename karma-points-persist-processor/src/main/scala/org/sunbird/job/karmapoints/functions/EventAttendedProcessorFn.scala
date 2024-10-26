package org.sunbird.job.karmapoints.functions

import com.datastax.driver.core.Row
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility._
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util
import java.util.Date

class EventAttendedProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                              (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config)   {

  private val logger = LoggerFactory.getLogger("org.sunbird.job.karmapoints.functions.EventAttendedProcessorFn")

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount, config.successEventCount,
      config.cacheHitCount, config.karmaPointsIssueEventsCount, config.cacheMissCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    val usrId = event.getMap().get(config.USER_UNDERSCORE_ID).asInstanceOf[String]
    val event_Id = event.getMap().get(config.EVENT_ID).asInstanceOf[String]
    val batch_Id = event.getMap().get(config.BATCH_ID).asInstanceOf[String]
    val ets = event.getMap().get(config.ETS).toString.toLong
    val etsDate = new java.util.Date(ets)
        //  val res :util.List[Row] = fetchUserBatch(event_Id,batch_Id)(config, cassandraUtil)
       //  val endDate = res.get(0).getObject(config.DB_COLUMN_END_DATE).asInstanceOf[Date]
      //  if(etsDate.after(endDate))
     //    return
    if(doesEntryExist(usrId,config.CONTEXT_TYPE_EVENT,config.OPERATION_TYPE_EVENT,event_Id)(metrics,config, cassandraUtil)) {
       logger.info("The Karma Points entry already exists for eventId: " + event_Id)
      return
    }
    val headers = Map(
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON,
    )
    val (eventName,endDate)  = fetchEventName(event_Id,headers)(metrics, config, httpUtil)
      //  val res :util.List[Row] = fetchUserBatch(event_Id,batch_Id)(config, cassandraUtil)
     //  val endDate = res.get(0).getObject(config.DB_COLUMN_END_DATE).asInstanceOf[Date]
    if(etsDate.after(endDate)) {
       logger.info("Karma Points were not allocated as the event's endDate " + endDate + " has passed, and the event was consumed on " + etsDate)
      return
    }
    kpOnUserEventAttended(usrId, config.CONTEXT_TYPE_EVENT, config.OPERATION_TYPE_EVENT, event_Id, eventName, etsDate.getTime)(metrics)
  }

  private def kpOnUserEventAttended(userId: String, contextType: String, operationType: String, contextId: String, eventName: String, creditDate: Long)(metrics: Metrics): Unit = {
    val points: Int = config.eventQuotaKarmaPoints
    val addInfoMap = new util.HashMap[String, AnyRef]
    addInfoMap.put(config.ADDINFO_EVENTNAME, eventName)
    var addInfo = config.EMPTY
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    insertKarmaPoints(userId, contextType, operationType, contextId, points, addInfo, creditDate)(metrics, config, cassandraUtil)
    updateKarmaSummary(userId, points)( config, cassandraUtil)
  }
}
