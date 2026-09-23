package org.sunbird.job.collectioncert.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.common.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.collectioncert.domain.Event
import org.sunbird.job.collectioncert.task.CollectionCertPreProcessorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.util.UUID
import scala.collection.JavaConverters._

class CollectionCertPreProcessorFn(config: CollectionCertPreProcessorConfig, httpUtil: HttpUtil)
                                  (implicit val stringTypeInfo: TypeInformation[String],
                                   @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessKeyedFunction[String, Event, String](config) with IssueCertificateHelper with IssueEventCertificateHelper {

    private[this] val logger = LoggerFactory.getLogger(classOf[CollectionCertPreProcessorFn])
    private var cache: DataCache = _
    private var contentCache: DataCache = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        val redisConnect = new RedisConnect(config)
        cache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
        cache.init()

      val metaRedisConn = new RedisConnect(config, Option(config.metaRedisHost), Option(config.metaRedisPort))
      contentCache = new DataCache(config, metaRedisConn, config.contentCacheStore, List())
      contentCache.init()
    }

    override def close(): Unit = {
        cassandraUtil.close()
        cache.close()
        super.close()
    }

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount, config.successEventCount,
            config.cacheHitCount)
    }

    override def processElement(event: Event,
                                context: KeyedProcessFunction[String, Event, String]#Context,
                                metrics: Metrics): Unit = {
        try {
            metrics.incCounter(config.totalEventsCount)
            if(event.isValid()(config)) {
              val certTemplates = fetchTemplates(event)(metrics).filter(template => template._2.getOrElse("url", "").asInstanceOf[String].contains(".svg"))
              if(!certTemplates.isEmpty) {
                certTemplates.map(template => {
                  val certEvent = issueCertificate(event, template._2)(cassandraUtil, cache, contentCache, metrics, config, httpUtil)
                  Option(certEvent).map(e => {
                    context.output(config.generateCertificateOutputTag, certEvent)
                    metrics.incCounter(config.successEventCount)}
                  ).getOrElse({metrics.incCounter(config.skippedEventCount)})
                })
              } else {
                logger.info(s"No certTemplates available for batchId :${event.batchId}")
                metrics.incCounter(config.skippedEventCount)
              }
            } else if (event.isValidEventType()(config)) {
                // Call necessary methods from new helper class
                val certTemplates = fetchTemplatesForEvent(event)(metrics).filter(template => template._2.getOrElse("url", "").asInstanceOf[String].contains(".svg"))
                if (!certTemplates.isEmpty) {
                    certTemplates.map(template => {
                        val certEvent = issueEventCertificate(event, template._2)(cassandraUtil, cache, contentCache, metrics, config, httpUtil)
                        Option(certEvent).map(e => {
                            context.output(config.generateEventCertificateOutputTag, certEvent)
                            metrics.incCounter(config.successEventCount)
                        }
                        ).getOrElse({
                            metrics.incCounter(config.skippedEventCount)
                        })
                    })
                } else {
                    logger.info(s"No certTemplates available for Event batchId :${event.batchId}")
                    metrics.incCounter(config.skippedEventCount)
                }
            } else {
                logger.info(s"Invalid request : ${event}")
                metrics.incCounter(config.skippedEventCount)
            }
        } catch {
            case ex: Exception => {
                val failedEvent = generateFailedEvent(event)
                logger.info(s"Collection cert Pre Processor failed for event: $failedEvent")
                context.output(config.generateCertificateFailedOutputTag, failedEvent)
                metrics.incCounter(config.failedEventCount)
                logger.error(s"Error processing event for userId: ${event.userId}, courseId: ${event.courseId}, batchId: ${event.batchId}, partition: ${event.partition}, offset: ${event.offset}", ex)
            }
        }
        
        
    }
    
    def fetchTemplates(event: Event)(implicit metrics: Metrics): Map[String, Map[String, String]] = {
        val query = QueryBuilder.select(config.certTemplates).from(config.keyspace, config.courseTable)
          .where(QueryBuilder.eq(config.dbCourseId, event.courseId)).and(QueryBuilder.eq(config.dbBatchId, event.batchId))
        
        val row: Row = cassandraUtil.findOne(query.toString)
        if(null != row && !row.isNull(config.certTemplates)) {
            val templates = row.getMap(config.certTemplates, TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
            templates.asScala.map(template => (template._1 -> template._2.asScala.toMap)).toMap
        }else {
            Map[String, Map[String, String]]()
        }
    }

    def fetchTemplatesForEvent(event: Event)(implicit metrics: Metrics): Map[String, Map[String, String]] = {
        val query = QueryBuilder.select(config.certTemplates).from(config.keyspace, config.eventTable)
          .where(QueryBuilder.eq(config.dbEventId, event.eventId)).and(QueryBuilder.eq(config.dbBatchId, event.batchId))

        val row: Row = cassandraUtil.findOne(query.toString)
        if (null != row && !row.isNull(config.certTemplates)) {
            val templates = row.getMap(config.certTemplates, TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
            templates.asScala.map(template => (template._1 -> template._2.asScala.toMap)).toMap
        } else {
            Map[String, Map[String, String]]()
        }
    }

    def generateFailedEvent(event: Event): String = {
        val ets = System.currentTimeMillis
        val mid = s"LP.${ets}.${UUID.randomUUID}"
        val eventString = s"""{"eid": "BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "Course Certificate Generator", "type": "System"}, "context": {"pdata": {"ver": "1.0", "id": "org.sunbird.platform"}}, "object": {"id": "${event.batchId}_${event.courseId}", "type": "ProgramCertificatePreProcessorGeneration"}, "edata": {"userId": "[${event.userId}]", "action": "issue-certificate", "iteration": 1, "trigger": "auto-issue", "batchId": "${event.batchId}", "completedLanguage": ["${event.completedLanguage}"], "courseId": "${event.courseId}"}}"""
        eventString
    }
}
