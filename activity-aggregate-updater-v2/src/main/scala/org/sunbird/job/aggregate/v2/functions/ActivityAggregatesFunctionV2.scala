package org.sunbird.job.aggregate.v2.functions

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement, TypeTokens}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.aggregate.v2.common.{ContentHelperV2, DeDupHelperV2, JsonKeys}
import org.sunbird.job.aggregate.v2.domain.{ContentDetail, ContentStatus, Event, UserActivityAgg, UserContentConsumption}
import org.sunbird.job.aggregate.v2.task.ActivityAggregateUpdaterConfigV2
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.util.{CassandraUtil, HttpUtil}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import java.util.{Map => JMap}

class ActivityAggregatesFunctionV2(config: ActivityAggregateUpdaterConfigV2,
                                   httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
  extends KeyedProcessFunction[String, Event, Event] with ContentHelperV2 {

  @transient private var metrics: Metrics = _
  private[this] val logger = LoggerFactory.getLogger(classOf[ActivityAggregatesFunctionV2])
//  private var cache: DataCache = _
  private var contentCache: DataCache = _
  var deDupEngine: DeDupEngine = _

  override def open(parameters: Configuration): Unit = {
    if (cassandraUtil == null)
      cassandraUtil = new CassandraUtil(
        config.dbHost,
        config.dbPort,
        config.cassandraReadTimeoutMs,
        config.cassandraConnectTimeoutMs,
        config.cassandraMaxRetries
      )
    /*cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()*/

    contentCache = new DataCache(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.contentStoreIndex, List())
    contentCache.init()

    metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]())
    deDupEngine = new DeDupEngine(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.deDupStore, config.deDupExpirySec)
    deDupEngine.init()
  }

  override def processElement(event: Event,
                              ctx: KeyedProcessFunction[String, Event, Event]#Context,
                              out: Collector[Event]): Unit = {
    // Exceptions are intentionally NOT caught here.
    // Any failure (Cassandra timeout, malformed data, etc.) propagates to Flink,
    // which triggers a task restart via the configured fixedDelayRestart strategy.
    // This ensures no events are silently dropped and issues are detected quickly.
    val userId = event.userId
    val courseId = event.courseId
    val batchId = event.batchId
    val language = event.language
    val contents = event.contents

    val (isValid, category) = verifyPrimaryCategory(courseId)(metrics, config, httpUtil, contentCache)
    if (!isValid) return

    val enrolmentRow = getEnrolment(userId, courseId, batchId)
    var langContentStatus: Map[String, Map[String, Int]] =
      if (enrolmentRow != null && enrolmentRow.getObject("lang_contentstatus") != null) {
        enrolmentRow.getObject("lang_contentstatus")
          .asInstanceOf[JMap[String, JMap[String, Integer]]]
          .asScala
          .map { case (lang, contentMap) =>
            lang -> contentMap.asScala.toMap.mapValues(_.intValue())
          }.toMap
      } else {
        Map.empty
      }

    if (!langContentStatus.contains(language)) {
      langContentStatus = langContentStatus + (language -> Map.empty[String, Int])
    }

    val existingLangMap = langContentStatus.getOrElse(language, Map.empty[String, Int])

    val courseMetadataJava = getCourseInfo(courseId)(metrics, config, contentCache, httpUtil)
    val courseMetadata = courseMetadataJava.asScala.toMap

    val translatedLeafNodes = getTranslatedLeafNodes(language, courseMetadata, contents.headOption.map(_.contentId).getOrElse(""), courseId)
    val updatedMap = updateContentStatuses(event, contents, existingLangMap, translatedLeafNodes)
    val updatedLangMap = contents.foldLeft(existingLangMap) { (acc, content) =>
      val contentId = content.contentId
      val incomingStatus = content.status
      val existingStatus = acc.getOrElse(contentId, 0)
      if (!acc.contains(contentId)) acc + (contentId -> incomingStatus)
      else if (incomingStatus > existingStatus) acc + (contentId -> incomingStatus)
      else acc
    }

    val statusIsTwo = enrolmentRow != null && enrolmentRow.getInt("status").equals(2)

    val finalLangContentStatus = updateLangContentStatusInUserEnrolment(
      userId, courseId, batchId, language, langContentStatus, updatedLangMap, courseMetadata, statusIsTwo
    )
    if (JsonKeys.LEARNING_PATHWAY.equalsIgnoreCase(category)) {
      evaluateLearnerPathwayCompletion(event, courseMetadata, ctx)
    } else {
      triggerCertificateIfRequired(event, courseMetadata, finalLangContentStatus(language), ctx)
    }
    out.collect(event)
  }

  def triggerCertificateIfRequired( event: Event,
                                    courseMetadata: Map[String, AnyRef],
                                    langContentStatus: Map[String, Int],
                                    ctx: KeyedProcessFunction[String, Event, Event]#Context
                                  ): Unit = {

    val languageMap = courseMetadata
      .getOrElse("languageMapV1", Map.empty[String, Map[String, AnyRef]])
      .asInstanceOf[Map[String, Map[String, AnyRef]]]

    val translatedCourseId = languageMap
      .get(event.language)
      .flatMap(_.get("id"))
      .map(_.toString)
      .getOrElse(event.courseId)

    val translatedLeafNodes = getLeafNodes(translatedCourseId)(metrics).toSet
    val completedCount = translatedLeafNodes.count(cid => langContentStatus.getOrElse(cid, 0) == 2)
    val batchid = event.batchId
    if (translatedLeafNodes.nonEmpty) {
      val userAgg = UserActivityAgg(
        activity_type = "Course",
        user_id = event.userId,
        activity_id = event.courseId,
        context_id = s"cb:$batchid",
        aggregates = Map("completedCount" -> completedCount.toDouble),
        agg_last_updated = Map("completedCount" -> System.currentTimeMillis())
      )

      val updateAggQuery = getUserAggQuery(userAgg)
      // LOCAL_QUORUM: ensure the agg write reaches majority of nodes before
      // the next event can read a stale aggregates value.
      updateAggQuery.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      cassandraUtil.update(updateAggQuery)
    }
    if (config.dedupEnabled) {
      event.contents
        .filter(_.status == 2)
        .map(c => DeDupHelperV2.getMessageId(event.courseId, event.batchId, event.userId, c.contentId, 2, event.language))
        .foreach(cs => deDupEngine.storeChecksum(cs))
    }

    if (translatedLeafNodes.nonEmpty && completedCount == translatedLeafNodes.size) {
      createIssueCertEvent(event.userId, event.courseId, event.batchId, event.language, ctx)(metrics)
    } else {
      logger.info(s"Certificate not triggered. Completed: $completedCount / ${translatedLeafNodes.size}")
    }
  }

  def updateContentStatuses( event: Event,
                             contents: List[ContentDetail],
                             existingLangMap: Map[String, Int],
                             translatedLeafNodes: Set[String]
                           ): Map[String, Int] = {

    var updatedMap = existingLangMap

    contents.foreach { content =>
      val contentId = content.contentId
      val incomingStatus = content.status
      val existingStatus = existingLangMap.getOrElse(contentId, 0)
      // Proceed only if it's a valid translated leaf node and incoming is higher than existing
      if ((existingLangMap.isEmpty && translatedLeafNodes.contains(contentId)) || incomingStatus > existingStatus) {

        // Read from DB to compare actual persisted status
        val existingRow = readContentConsumption(event.userId, event.courseId, event.batchId, event.language, contentId)
        val dbStatus = existingRow.getOrElse("status", 0).asInstanceOf[Int]

        // Skip update if DB status is already 2 (complete)
        if (dbStatus < 2 && incomingStatus > dbStatus) {
          val finalStatus = incomingStatus
          val completedCount = if (finalStatus == 2) 1 else 0
          val viewCount = existingRow.getOrElse("viewcount", 0).asInstanceOf[Int] + 1

          val updateQuery = QueryBuilder.update(config.dbKeyspace, config.dbUserContentConsumptionTable)
            .`with`(QueryBuilder.set("status", finalStatus))
            .and(QueryBuilder.set("completedcount", completedCount))
            .and(QueryBuilder.set("viewcount", viewCount))
            .where(QueryBuilder.eq("userid", event.userId))
            .and(QueryBuilder.eq("courseid", event.courseId))
            .and(QueryBuilder.eq("batchid", event.batchId))
            .and(QueryBuilder.eq("language", event.language))
            .and(QueryBuilder.eq("contentid", contentId))

          cassandraUtil.update(updateQuery)

          updatedMap += (contentId -> finalStatus)
        }
      }
    }

    updatedMap
  }

  def getTranslatedLeafNodes(language: String, courseMetadata: Map[String, AnyRef], contentId: String, courseId: String): Set[String] = {
    val languageMapV1 = courseMetadata
      .getOrElse("languageMapV1", Map.empty[String, AnyRef])
      .asInstanceOf[Map[String, Map[String, AnyRef]]]

    val courseLangs = courseMetadata.get("language").map {
      case l: java.util.List[_] => l.asInstanceOf[java.util.List[String]].asScala.toList.map(_.toLowerCase)
      case s: String            => List(s.toLowerCase)
      case _                    => List.empty[String]
    }.getOrElse(List.empty[String])

    if (courseLangs.contains(language.toLowerCase)) {
      return getLeafNodes(courseId)(metrics).toSet
    }

    val translatedCourseIdOpt = languageMapV1
      .get(language)
      .flatMap(_.get("id"))
      .map(_.toString)

    translatedCourseIdOpt match {
      case Some(translatedCourseId) =>
        getLeafNodes(translatedCourseId)(metrics).toSet
      case None =>
        val msg = s"Language '$language' not found in languageMapV1 and does not match course language for courseId: $courseId"
        logger.error(msg)
        throw new IllegalArgumentException(msg)
    }
  }


  def verifyPrimaryCategory(identifier: String)(
    metrics: Metrics,
    config: ActivityAggregateUpdaterConfigV2,
    httpUtil: HttpUtil,
    contentCache: DataCache
  ): (Boolean, String) = {
    logger.info(
      "Verify Program post-publish required for content: " + identifier
    )
    // Get the primary Categories for the courses here
    var isValidCourse = false
    var courseCategory = ""
    val excludedCategories = config.excludedCategories
    val contentObj: java.util.Map[String, AnyRef] =
      getCourseInfo(identifier)(metrics, config, contentCache, httpUtil)
    if (!contentObj.isEmpty) {
      val primaryCategory = contentObj.get("primaryCategory").asInstanceOf[String]
      courseCategory = contentObj.get("courseCategory").asInstanceOf[String]
      if (primaryCategory != null && !excludedCategories.contains(primaryCategory)) {
        isValidCourse = true
      }
      logger.info("PrimaryCategory value is : " + primaryCategory + ", for Id: " + identifier)
    } else {
      logger.error("Failed to read content details for Id: " + identifier)
      throw new Exception(s"Failed to read content for Id $identifier")
    }
    logger.info("is activity aggregator is processing this event ? " + isValidCourse)
    (isValidCourse, courseCategory)
  }

  def updateUserEnrolmentLangStatus(
                                     userId: String,
                                     courseId: String,
                                     batchId: String,
                                     langMap: Map[String, Map[String, Int]],
                                     completionPercentage: Int,
                                     progress: Int,
                                     isCompleted: Boolean, isStatusTwo: Boolean
                                   ): Unit = {
    val mapForCassandra: JMap[String, JMap[String, Integer]] = langMap.map {
      case (language, contentMap) =>
        language -> contentMap.map {
          case (contentId, status) => contentId -> Int.box(status)
        }.asJava
    }.asJava
    var assignments = QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set("lang_contentstatus", mapForCassandra))
      .and(QueryBuilder.set("progress", progress))
      .and(QueryBuilder.set("datetime", System.currentTimeMillis()))
      .and(QueryBuilder.set("completionPercentage", completionPercentage))

    if (!isStatusTwo) {
      assignments = assignments.and(QueryBuilder.set("status", if (isCompleted) 2 else 1))
    }

    if (isCompleted && !isStatusTwo)
      assignments.and(QueryBuilder.set("completedon", new java.util.Date()))

    val update = assignments
      .where(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
    logger.info(s"Updating user enrolment for user: $userId, course: $courseId, batch: $batchId with lang_contentstatus: $langMap")
    // LOCAL_QUORUM: ensure this write is visible to at least 2/3 nodes before
    // any subsequent read (which also uses LOCAL_QUORUM) can proceed.
    update.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.update(update)
  }


  def readContentConsumption(
                              userId: String,
                              courseId: String,
                              batchId: String,
                              language: String,
                              contentId: String
                            ): Map[String, AnyRef] = {

    val query = QueryBuilder.select()
      .all()
      .from(config.dbKeyspace, config.dbUserContentConsumptionTable)
      .where(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
      .and(QueryBuilder.eq("language", language))
      .and(QueryBuilder.eq("contentid", contentId))
      .limit(1)

    val rows = cassandraUtil.find(query.toString).asScala

    if (rows.nonEmpty) {
      val row = rows.head
      Map(
        "status" -> Int.box(Option(row.getObject("status")).map(_.asInstanceOf[Int]).getOrElse(0)),
        "viewcount" -> Int.box(Option(row.getObject("viewcount")).map(_.asInstanceOf[Int]).getOrElse(0)),
        "completedcount" -> Int.box(Option(row.getObject("completedcount")).map(_.asInstanceOf[Int]).getOrElse(0)),
        "last_access_time" -> Option(row.getTimestamp("last_access_time")).orNull
      )
    } else {
      Map.empty[String, AnyRef]
    }
  }

  def getLeafNodes(courseId: String)(implicit metrics: Metrics): List[String] = {
    try {
      val courseInfo = getCourseInfo(courseId)(metrics, config, contentCache, httpUtil)
      if (courseInfo != null && courseInfo.containsKey("leafNodes")) {
        courseInfo.get("leafNodes").asInstanceOf[java.util.List[String]].asScala.toList
      } else {
        List.empty[String]
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error fetching leafNodes for courseId: $courseId", ex)
        List.empty[String]
    }
  }

  def createIssueCertEvent(userId: String, courseId: String, batchId: String, completedLanguage: String, context: KeyedProcessFunction[String, Event, Event]#Context)(implicit metrics: Metrics): Unit = {
    val ets = System.currentTimeMillis()
    val mid = s"LP.$ets.${UUID.randomUUID().toString}"
    val event =
      s"""{
         |"eid": "BE_JOB_REQUEST",
         |"ets": $ets,
         |"mid": "$mid",
         |"actor": { "id": "Course Certificate Generator", "type": "System" },
         |"context": { "pdata": { "ver": "1.0", "id": "org.sunbird.platform" } },
         |"object": { "id": "${batchId}_$courseId", "type": "CourseCertificateGeneration" },
         |"edata": {
         |  "userIds": ["$userId"],
         |  "action": "issue-certificate",
         |  "iteration": 1,
         |  "trigger": "auto-issue",
         |  "batchId": "$batchId",
         |  "reIssue": false,
         |  "courseId": "$courseId",
         |  "completedLanguage": "$completedLanguage"
         |}
         |}""".stripMargin

    logger.info(s"Issuing cert for user: $userId in batch: $batchId course: $courseId")
    context.output(config.certIssueOutputTag, event)
  }

  def updateLangContentStatusInUserEnrolment(
                                              userId: String,
                                              courseId: String,
                                              batchId: String,
                                              language: String,
                                              langContentStatus: Map[String, Map[String, Int]],
                                              updatedLangMap: Map[String, Int],
                                              courseMetadata: Map[String, AnyRef],
                                              statusIsTwo: Boolean
                                            ): Map[String, Map[String, Int]] = {

    val languageMap = courseMetadata
      .getOrElse("languageMapV1", Map.empty[String, Map[String, AnyRef]])
      .asInstanceOf[Map[String, Map[String, AnyRef]]]

    val translatedCourseId = languageMap
      .get(language)
      .flatMap(_.get("id"))
      .map(_.toString)
      .getOrElse(courseId)

    val translatedLeafNodes = getLeafNodes(translatedCourseId)(metrics).toSet

    val completedCount = translatedLeafNodes.count(cid => updatedLangMap.getOrElse(cid, 0) == 2)

    val progress = completedCount

    val leafNodesCount = translatedLeafNodes.size

    val completionPercentage: Int =
      if (leafNodesCount > 0) {
        Math.min(
          100,
          Math.round((completedCount.toFloat / leafNodesCount.toFloat) * 100)
        )
      } else {
        0
      }

    val finalLangContentStatus = langContentStatus + (language -> updatedLangMap)

    val isCompleted = translatedLeafNodes.nonEmpty && completedCount == translatedLeafNodes.size

    updateUserEnrolmentLangStatus(userId, courseId, batchId, finalLangContentStatus, completionPercentage, progress, isCompleted, statusIsTwo)

    finalLangContentStatus
  }

  def getUserAggQuery(progress: UserActivityAgg): Update.Where = {
    QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll("aggregates", progress.aggregates.asJava))
      .and(QueryBuilder.putAll("agg_last_updated", progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq("activity_id", progress.activity_id))
      .and(QueryBuilder.eq("activity_type", progress.activity_type))
      .and(QueryBuilder.eq("context_id", progress.context_id))
      .and(QueryBuilder.eq("user_id", progress.user_id))
  }

  override def close(): Unit = {
    if (cassandraUtil != null) cassandraUtil.close()
    //cache.close()
    contentCache.close()
    super.close()
  }

  def getEnrolment(userId: String, courseId: String, batchId: String) = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .where()
    selectWhere
      .and(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
    // LOCAL_QUORUM: require majority of replica nodes to respond so we never
    // read stale data that hasn't yet been replicated from a prior write.
    val stmt = new SimpleStatement(selectWhere.toString)
      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.findOneWithStatement(stmt)
  }

  def evaluateLearnerPathwayCompletion(
                                        event: Event,
                                        courseMetadata: Map[String, AnyRef],
                                        ctx: KeyedProcessFunction[String, Event, Event]#Context
                                      ): Unit = {


    val milestones: List[Map[String, AnyRef]] =
      courseMetadata
        .get(JsonKeys.MILESTONES_V1)
        .collect {
          case l: List[Map[String, AnyRef]] => l
          case jl: java.util.List[java.util.Map[String, AnyRef]] =>
            jl.asScala.map(_.asScala.toMap).toList
        }
        .getOrElse(List.empty)

    if (milestones.isEmpty) {
      logger.info("LP evaluation skipped: milestones_v1 not found")
      return
    }

    val lpEnrolmentRow =
      getEnrolment(event.userId, event.courseId, event.batchId)

    if (lpEnrolmentRow == null) {
      logger.info("LP enrolment not found, skipping evaluation")
      return
    }

    val issuedCertObj = lpEnrolmentRow.getObject(JsonKeys.ISSUED_CERTIFICATES)

    val hasIssuedCertificate = issuedCertObj match {
      case null => false

      case m: java.util.Map[_, _] =>
        !m.isEmpty

      case l: java.util.List[_] =>
        !l.isEmpty

      case _ =>
        false
    }

    if (hasIssuedCertificate) {
      logger.info(
        s"LP certificate already issued for user ${event.userId}, skipping trigger"
      )
      return
    }

    val langContentStatus: Map[String, Int] = {

      val result = scala.collection.mutable.Map[String, Int]()

      lpEnrolmentRow.getObject(JsonKeys.LANG_CONTENT_STATUS) match {

        case outer: java.util.Map[_, _] =>
          outer.forEach { (k, v) =>
            if (k != null && k.toString == event.language) {
              v match {
                case inner: java.util.Map[_, _] =>
                  inner.forEach {
                    case (ik, iv: Integer) if ik != null =>
                      result.put(ik.toString, iv.toInt)
                    case _ =>
                  }
                case _ =>
              }
            }
          }

        case _ =>
      }

      result.toMap
    }

    val allMilestonesCompleted =
      milestones.forall { milestone =>
        isMilestoneCompleted(
          event.userId,
          milestone,
          langContentStatus
        )
      }

    if (!allMilestonesCompleted) {
      logger.info(
        s"LP not completed for user ${event.userId} – milestone courses or assessments pending"
      )
      return
    }

    val areMilestoneAssessmentsCompleted =
      milestones.forall { milestone =>
        milestone
          .get(JsonKeys.ASSESSMENT_DETAILS)
          .collect {
            case m: Map[String, AnyRef] =>
              m.get(JsonKeys.IDENTIFIER).map(_.toString)
          }
          .flatten
          .forall { assessmentId =>
            langContentStatus.get(assessmentId).contains(2)
          }
      }

    if (!areMilestoneAssessmentsCompleted) {
      logger.info(
        s"LP not completed for user ${event.userId} – milestone assessment pending"
      )
      return
    }

    val isPreEnrolmentAssessmentCompleted: Boolean =
      courseMetadata
        .get(JsonKeys.PRE_LIMINARY_ASSESSMENT)
        .collect { case id: String => id }
        .exists { id =>
          langContentStatus.get(id).contains(2)
        }


    if (!isPreEnrolmentAssessmentCompleted) {
      logger.info(
        s"LP not completed for user ${event.userId} – pre-enrolment assessment pending"
      )
      return
    }

    createIssueCertEvent(
      event.userId,
      event.courseId,
      event.batchId,
      event.language,
      ctx
    )(metrics)

    markLPCompleted(
      event.userId,
      event.courseId,
      event.batchId
    )
  }

  def isMilestoneCompleted(
                            userId: String,
                            milestone: Map[String, AnyRef],
                            langContentStatus: Map[String, Int]
                          ): Boolean = {

    val milestoneId = milestone.getOrElse("id","")

    val courses: List[Map[String, AnyRef]] =
      milestone
        .get(JsonKeys.COURSES)
        .collect {
          case l: List[Map[String, AnyRef]] => l
          case jl: java.util.List[java.util.Map[String, AnyRef]] =>
            jl.asScala.map(_.asScala.toMap).toList
        }
        .getOrElse(List.empty)

    val mandatoryCourseIds =
      courses
        .filter(_.get(JsonKeys.IS_MANDATORY).contains(true))
        .map(_(JsonKeys.IDENTIFIER).toString)

    if (mandatoryCourseIds.isEmpty) {
      logger.info(s"Milestone [$milestoneId] has no mandatory courses")
    }

    val areMandatoryCoursesCompleted =
      mandatoryCourseIds.isEmpty ||
        mandatoryCourseIds.forall { courseId =>
          val completed = isCourseCompleted(userId, courseId)
          logger.info(
            s"Milestone [$milestoneId] course [$courseId] completed = $completed"
          )
          completed
        }

    if (!areMandatoryCoursesCompleted) {
      logger.info(s"Milestone [$milestoneId] mandatory courses not completed")
      return false
    }

    val milestoneAssessmentId: Option[String] =
      milestone
        .get(JsonKeys.ASSESSMENT_DETAILS)
        .flatMap {
          case m: java.util.Map[_, _] =>
            Option(m.get(JsonKeys.IDENTIFIER)).map(_.toString)
          case _ =>
            None
        }


    val isMilestoneAssessmentCompleted =
      milestoneAssessmentId.forall { id =>
        langContentStatus.get(id).contains(2)
      }

    if (!isMilestoneAssessmentCompleted) {
      logger.info(
        s"Milestone [$milestoneId] assessment not completed"
      )
      return false
    }

    logger.info(s"Milestone [$milestoneId] completed successfully")
    true
  }

  def isCourseCompleted(
                         userId: String,
                         courseId: String
                       ): Boolean = {
    val enrolmentRow = getEnrolment(userId, courseId)
    if (enrolmentRow == null) return false
    val isCompleted = enrolmentRow.getInt(JsonKeys.STATUS) == 2

    val issuedCerts =
      enrolmentRow.getList(
        JsonKeys.ISSUED_CERTIFICATES,
        TypeTokens.mapOf(classOf[String], classOf[String])
      )

    val isCertificateIssued =
      issuedCerts != null && !issuedCerts.isEmpty

    isCompleted && isCertificateIssued
  }

  def markLPCompleted(
                       userId: String,
                       lpId: String,
                       batchId: String
                     ): Unit = {

    val updateQuery = QueryBuilder
      .update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set(JsonKeys.STATUS, 2))
      .and(QueryBuilder.set(JsonKeys.COMPLETED_ON_KEY, new java.util.Date()))
      .and(QueryBuilder.set(JsonKeys.DATE_TIME_KEY, System.currentTimeMillis()))
      .where(QueryBuilder.eq(JsonKeys.USER_ID_KEY, userId))
      .and(QueryBuilder.eq(JsonKeys.COURSE_ID_KEY, lpId))
      .and(QueryBuilder.eq(JsonKeys.BATCH_ID_KEY, batchId))

    cassandraUtil.update(updateQuery)

    logger.info(
      s"LP marked as completed (status=2) for user=$userId, lpId=$lpId, batchId=$batchId"
    )
  }

  def getEnrolment(userId: String, courseId: String) = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq(JsonKeys.USER_ID_KEY, userId))
      .and(QueryBuilder.eq(JsonKeys.COURSE_ID_KEY, courseId))
    cassandraUtil.findOne(selectWhere.toString)
  }

}