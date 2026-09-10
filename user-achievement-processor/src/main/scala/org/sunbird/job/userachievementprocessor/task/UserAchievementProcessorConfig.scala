package org.sunbird.job.userbadgeawarding.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

import java.util

class UserBadgeAwardingConfig(override val config: Config) extends BaseJobConfig(config, "user-badge-awarding-processor") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  //Redis config
  val collectionCacheStore: Int = 0
  val badgeCountCacheStore: Int = 2
  val badgeCacheStore: Int = 12

  // Content cache config
  val contentCacheExpiryMs: Long = config.getLong("content.cache.expiry.ms")

  val recentBadgeActivityKey: String = config.getString("redis.recentBadgeActivityKey")
  val recentBadgeActivityMaxSize: Int = config.getInt("redis.recentBadgeActivityMaxSize")


  //kafka config
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val generateBadgeFailedEventProducer = "generate-badge-failed-event-sink"
  val generateBadgeParallelism: Int = config.getInt("task.badge.parallelism")

  //Cassandra config
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val dbName: String = config.getString("lms-cassandra.sunbird.db")
  val dbTable: String = config.getString("lms-cassandra.sunbird.table")
  val coursesdb: String = config.getString("lms-cassandra.coursesdb")
  val enrolmentTable: String = config.getString("lms-cassandra.enrolmentTable")
  val badgeLookUpTable: String = config.getString("lms-cassandra.badgeLookupTable")
  val externalEnrolmentTable: String = config.getString("lms-cassandra.externalEnrolmentTable")
  val generateBadgeFailedOutputTagName = "generate-badge-failed-request"
  val generateBadgeFailedOutputTag: OutputTag[String] = OutputTag[String](generateBadgeFailedOutputTagName)

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbUpdateCount = "db-update-count"

  //Constants
  val defaultHeaders = Map[String, String]("Content-Type" -> "application/json")
  val userAccBlockedErrCode = "UOS_USRRED0006"

  // Content and External Content config
  val contentReadURL: String = config.getString("content.read.url")
  val contentHierarchyURL: String = config.getString("content.hierarchy.url")
  val extContentUrl: String = config.getString("extcontent.read.url")
  val extCoursesContextType: String = config.getString("extcontent.extCourses")
  val iGOTCoursesContextType: String = config.getString("extcontent.iGOTCourses")
  val curatedProgramContextType: String = config.getString("extcontent.curatedProgram")
  val extContentResponseKey: String = config.getString("extcontent.responseKey")
  val extContentUserExternalEnrolmentsIssuedCertificatesKey: String = config.getString("extcontent.issuedCertificatesKey")
  val lastIssuedOnKey: String = config.getString("extcontent.lastIssuedOnKey")
  val issuedCertificatesKey: String = config.getString("extcontent.issuedCertificatesKey")

  // Badge awarding config
  val badgeDetailsV1Key: String = config.getString("extcontent.badgeDetailsV1Key")
  val badgeEarningDateEnabledKey: String = config.getString("extcontent.badgeEarningDateEnabledKey")
  val badgeEarningDateTimeKey: String = config.getString("extcontent.badgeEarningDateTimeKey")
  val issuedBadgesKey: String = config.getString("extcontent.issuedBadgesKey")
  val criteriaKey: String = config.getString("extcontent.criteriaKey")
  val badgeIdKey: String = config.getString("extcontent.badgeIdKey")
  val issuedOnKey: String = config.getString("extcontent.issuedOnKey")
  val templateUrlKey: String = config.getString("extcontent.templateUrlKey")
  val badgeTemplateKey: String = config.getString("extcontent.badgeTemplateKey")

  // Notification config
  val notificationServiceUrl: String = config.getString("notification.service.url")
  val notificationBadgeSubCategory: String = config.getString("notification.badge.subCategory")
  val notificationBadgeSubType: String = config.getString("notification.badge.subType")
  val notificationEnabled: Boolean = config.getBoolean("notification.badge.enabled")
  val userId: String = "userid"
  val batchId: String = "batchid"
  val courseId: String = "courseid"
  val firstName: String = "firstname"
  val id: String = "id"
  val badgeId: String = "badgeid"
  val badgeTitle: String = "badgeTitle"
  val criteria: String = "criteria"
  val issuedOn: String = "issuedon"
  val templateUrl: String = "templateurl"
  val courseCategory: String = "courseCategory"
  val coursecategory: String = "coursecategory"
  val LEARNING_PATHWAY = "Learning Pathway"

  val badgeEnabledCourses: util.List[String] = if (config.hasPath("badge.enabled.courses")) config.getStringList("badge.enabled.courses") else util.Arrays.asList("")
  val badgeEnabledPrograms: util.List[String] = if (config.hasPath("badge.enabled.programs")) config.getStringList("badge.enabled.programs") else util.Arrays.asList("")

  val programHierarchyCacheTtl: Int = if (config.hasPath("program.hierarchy.cache.ttl")) config.getInt("program.hierarchy.cache.ttl") else 3600000
}
