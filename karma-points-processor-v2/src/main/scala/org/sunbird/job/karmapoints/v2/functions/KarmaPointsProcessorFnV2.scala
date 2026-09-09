package org.sunbird.job.karmapoints.v2.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.{DataQualityException, MissingEventTypeException, SystemException, UnknownEventTypeException}
import org.sunbird.job.karmapoints.v2.handlers._
import org.sunbird.job.karmapoints.v2.storage.{CassandraUtil, RedisUtil}
import org.sunbird.job.karmapoints.v2.utils.{ExternalServiceClient, FailedEventProducer, KarmaMetrics}
import org.sunbird.job.util.{HttpUtil, CassandraUtil => JobsCoreCassandraUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

/**
 * The single entry point V2 uses in place of V1's 7 separate `*ProcessorFn` classes. Keyed by
 * userId (see `KarmaPointsProcessorTaskV2`) so per-user events are processed in order on the same
 * subtask, which is what makes the read-before-write Cassandra idempotency checks in the storage
 * layer safe at parallelism > 1 - the same safety V1 got "for free" only because it ran at
 * parallelism=1 everywhere.
 *
 * Implements the 2-path error strategy: [[DataQualityException]] -> logged, published to the
 * failed-topic, event acknowledged (no rethrow, checkpoint offset commits normally).
 * [[SystemException]] (or any exception this class didn't anticipate) -> logged and rethrown,
 * which fails the Flink task and lets the configured restart strategy replay the event from the
 * last checkpoint.
 */
class KarmaPointsProcessorFnV2(config: KarmaPointsV2Config, httpUtil: HttpUtil)
  extends BaseProcessKeyedFunction[String, UnifiedEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsProcessorFnV2])

  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var redisUtil: RedisUtil = _
  @transient private var failedEventProducer: FailedEventProducer = _
  @transient private var externalServiceClient: ExternalServiceClient = _
  @transient private var karmaMetrics: KarmaMetrics = _

  @transient private var courseCompletionHandler: CourseCompletionHandler = _
  @transient private var ratingHandler: RatingEventHandler = _
  @transient private var firstEnrolmentHandler: FirstEnrolmentHandler = _
  @transient private var firstLoginHandler: FirstLoginHandler = _
  @transient private var acbpClaimHandler: ACBPClaimHandler = _
  @transient private var eventAttendedHandler: EventAttendedHandler = _
  @transient private var unenrolmentHandler: UnenrolmentHandler = _
  @transient private var pointsConversionHandler: PointsConversionHandler = _
  @transient private var coinsRedemptionHandler: CoinsRedemptionHandler = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val jobsCoreCassandraUtil = new JobsCoreCassandraUtil(config.dbHost, config.dbPort,
      config.cassandraReadTimeoutMs, config.cassandraConnectTimeoutMs, config.cassandraMaxRetries)
    cassandraUtil = new CassandraUtil(config, jobsCoreCassandraUtil)

    val redisConnect = new RedisConnect(config, Option(config.metaRedisHost), Option(config.metaRedisPort))
    val dataCache = new DataCache(config, redisConnect, config.cacheDbId, List())
    dataCache.init()
    redisUtil = new RedisUtil(dataCache, config)

    failedEventProducer = new FailedEventProducer(config)
    failedEventProducer.init()

    externalServiceClient = new ExternalServiceClient(config, httpUtil)

    courseCompletionHandler = new CourseCompletionHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    ratingHandler = new RatingEventHandler(config, cassandraUtil, redisUtil)
    firstEnrolmentHandler = new FirstEnrolmentHandler(config, cassandraUtil, redisUtil)
    firstLoginHandler = new FirstLoginHandler(config, cassandraUtil, redisUtil)
    acbpClaimHandler = new ACBPClaimHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    eventAttendedHandler = new EventAttendedHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    unenrolmentHandler = new UnenrolmentHandler(config, cassandraUtil, redisUtil)
    pointsConversionHandler = new PointsConversionHandler(config, cassandraUtil, redisUtil)
    coinsRedemptionHandler = new CoinsRedemptionHandler(config, cassandraUtil, redisUtil)
  }

  override def close(): Unit = {
    if (cassandraUtil != null) cassandraUtil.close()
    if (redisUtil != null) redisUtil.close()
    if (failedEventProducer != null) failedEventProducer.close()
    super.close()
  }

  override def metricsList(): List[String] = List(
    config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount,
    config.dbReadCount, config.dbUpdateCount, config.cacheHitCount, config.cacheMissCount,
    config.dataQualityErrorCount, config.systemErrorCount
  )

  override def processElement(event: UnifiedEvent,
                              context: KeyedProcessFunction[String, UnifiedEvent, String]#Context,
                              metrics: Metrics): Unit = {
    // `metrics` is the same instance across calls for this operator instance (BaseProcessKeyedFunction
    // registers it once), so it's safe to lazily wrap it once and keep accumulating into it.
    if (karmaMetrics == null) karmaMetrics = new KarmaMetrics(config, metrics)
    val startNanos = System.nanoTime()
    karmaMetrics.incCounter(config.totalEventsCount)
    // TODO: Remove temporary INFO log after testing.
    logger.info(
      s"Processing karma event: userId=${extractUserId(event)}, eventType=${event.eventType}"
    )

    try {
      validateEvent(event)
      routeEvent(event)(metrics)
      karmaMetrics.incCounter(config.successEventCount)
      karmaMetrics.incEventTypeCounter(event.eventType)
      // TODO: Remove temporary INFO log after testing.
      logger.info(
        s"Karma event processed successfully: userId=${extractUserId(event)}, eventType=${event.eventType}"
      )
    } catch {
      case ex: DataQualityException =>
        logger.warn(s"Data-quality failure, routing to failed-topic: userId=${extractUserId(event)}, " +
          s"eventType=${event.eventType}, reason=${ex.message}")
        karmaMetrics.incCounter(config.failedEventCount)
        karmaMetrics.incDataQualityError(ex.getClass.getSimpleName)
        sendToFailedTopic(event, ex)
        // Deliberately no rethrow: the event is acknowledged and the checkpoint offset commits,
        // so a bad event never blocks the partition or gets redelivered forever.

      case ex: SystemException =>
        logger.error(s"System failure, rethrowing to trigger job restart: userId=${extractUserId(event)}, " +
          s"eventType=${event.eventType}, reason=${ex.message}", ex.cause.getOrElse(ex))
        karmaMetrics.incSystemError(ex.getClass.getSimpleName)
        throw ex

      case ex: Exception =>
        // Anything not already classified is treated as infra/unknown and fails safe: rethrow so
        // Flink restarts rather than silently routing an unanticipated bug to the failed-topic.
        logger.error(s"Unexpected exception processing event: userId=${extractUserId(event)}, eventType=${event.eventType}", ex)
        karmaMetrics.incSystemError(ex.getClass.getSimpleName)
        throw ex
    } finally {
      karmaMetrics.recordLatency(startNanos)
    }
  }

  /**
   * Used only for log messages - not for validation or routing. RATING's and EVENT_ATTENDED's V1
   * payloads key their user id as data.user_id, FIRST_ENROLMENT's and ACBP_CLAIM's as
   * data.edata.userId, FIRST_LOGIN's as data.edata.id, UNENROLMENT's as data.edata.userIds,
   * COURSE_COMPLETION's as edata.userIds[0] (a JSON array, unwrapped - no `data` nesting for this
   * type); every other/unknown event type keeps the original top-level-userId-then-edata.userId
   * fallback (defensive - keyBy/extractUserId run before validateEvent, so eventType may be empty).
   */
  private def extractUserId(event: UnifiedEvent): String = event.eventType match {
    case config.EVENT_TYPE_RATING | config.EVENT_TYPE_EVENT_ATTENDED => event.dataString("user_id")
    case config.EVENT_TYPE_FIRST_ENROLMENT | config.EVENT_TYPE_ACBP_CLAIM => event.dataEdataString("userId")
    case config.EVENT_TYPE_FIRST_LOGIN => event.dataEdataString("id")
    case config.EVENT_TYPE_UNENROLMENT => event.dataEdataString("userIds")
    case config.EVENT_TYPE_COURSE_COMPLETION => event.edataStringArrayFirst("userIds")
    case config.EVENT_TYPE_POINTS_CONVERSION | config.EVENT_TYPE_COINS_REDEMPTION => event.dataString("userId")
    case _ =>
      val topLevel = event.userId
      if (StringUtils.isNotEmpty(topLevel)) topLevel else event.edataString("userId")
  }

  private def validateEvent(event: UnifiedEvent): Unit = {
    if (StringUtils.isEmpty(event.eventType)) {
      logger.warn(
        s"Invalid event: eventType is missing/empty, mid=${event.mid()}"
      )
      throw MissingEventTypeException(
        s"eventType is missing/empty, mid=${event.mid()}"
      )
    }
  }

  private def routeEvent(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    event.eventType match {
      case config.EVENT_TYPE_COURSE_COMPLETION => courseCompletionHandler.handle(event)
      case config.EVENT_TYPE_RATING => ratingHandler.handle(event)
      case config.EVENT_TYPE_FIRST_ENROLMENT => firstEnrolmentHandler.handle(event)
      case config.EVENT_TYPE_FIRST_LOGIN => firstLoginHandler.handle(event)
      case config.EVENT_TYPE_ACBP_CLAIM => acbpClaimHandler.handle(event)
      case config.EVENT_TYPE_EVENT_ATTENDED => eventAttendedHandler.handle(event)
      case config.EVENT_TYPE_UNENROLMENT => unenrolmentHandler.handle(event)
      case config.EVENT_TYPE_POINTS_CONVERSION => pointsConversionHandler.handle(event)
      case config.EVENT_TYPE_COINS_REDEMPTION => coinsRedemptionHandler.handle(event)
      case other => throw UnknownEventTypeException(s"Unknown eventType: '$other' for userId=${event.userId}")
    }
  }

  private def sendToFailedTopic(event: UnifiedEvent, ex: DataQualityException): Unit = {
    failedEventProducer.send(event, ex.getClass.getSimpleName, ex.message)
  }

  /**
   * Test-only seam: `open()` builds real Cassandra/Redis connections, which unit tests can't
   * stand up. Package-visible so [[org.sunbird.job.karmapoints.v2.UnifiedProcessorFnTest]] can
   * inject mocks and exercise validateEvent/routeEvent/error-handling without live infra.
   */
  private[v2] def initForTest(cassandraUtil: CassandraUtil, redisUtil: RedisUtil,
                              failedEventProducer: FailedEventProducer, externalServiceClient: ExternalServiceClient): Unit = {
    this.cassandraUtil = cassandraUtil
    this.redisUtil = redisUtil
    this.failedEventProducer = failedEventProducer
    this.externalServiceClient = externalServiceClient
    this.courseCompletionHandler = new CourseCompletionHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    this.ratingHandler = new RatingEventHandler(config, cassandraUtil, redisUtil)
    this.firstEnrolmentHandler = new FirstEnrolmentHandler(config, cassandraUtil, redisUtil)
    this.firstLoginHandler = new FirstLoginHandler(config, cassandraUtil, redisUtil)
    this.acbpClaimHandler = new ACBPClaimHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    this.eventAttendedHandler = new EventAttendedHandler(config, cassandraUtil, redisUtil, externalServiceClient)
    this.unenrolmentHandler = new UnenrolmentHandler(config, cassandraUtil, redisUtil)
    this.pointsConversionHandler = new PointsConversionHandler(config, cassandraUtil, redisUtil)
    this.coinsRedemptionHandler = new CoinsRedemptionHandler(config, cassandraUtil, redisUtil)
  }

  /**
   * Test-only accessors so [[org.sunbird.job.karmapoints.v2.EventRoutingSpec]] can assert routing
   * reached the right handler instance without exposing mutable handler fields more broadly.
   */
  private[v2] def pointsConversionHandlerForTest: PointsConversionHandler = pointsConversionHandler

  private[v2] def coinsRedemptionHandlerForTest: CoinsRedemptionHandler = coinsRedemptionHandler
}
