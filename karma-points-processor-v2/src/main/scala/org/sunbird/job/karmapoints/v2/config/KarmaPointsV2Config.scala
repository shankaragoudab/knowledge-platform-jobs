package org.sunbird.job.karmapoints.v2.config

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class KarmaPointsV2Config(override val config: Config) extends BaseJobConfig(config, "program-karma-points-processor-v2") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  val karmaPointsV2Consumer: String = "karma-points-unified-v2-consumer"
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  val failedEventOutputTag: OutputTag[String] = OutputTag[String]("karma-points-v2-failed-events")

  // Cassandra keyspaces/tables (same schema as V1 - no breaking changes)
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  val sunbird_keyspace: String = config.getString("cassandra.sunbird.keyspace")
  val sunbird_courses_keyspace: String = config.getString("cassandra.sunbird_courses.keyspace")
  val content_hierarchy_KeySpace: String = config.getString("cassandra.content_hierarchy.keyspace")

  val content_hierarchy_table: String = config.getString("cassandra.content_hierarchy.table")
  val user_karma_points_table: String = config.getString("cassandra.user_karma_points.table")
  val user_karma_points_credit_lookup_table: String = config.getString("cassandra.user_karma_points_credit_lookup.table")
  val user_enrollments_lookup_table: String = config.getString("cassandra.user_enrolments.table")
  val user_table: String = config.getString("cassandra.user.table")
  val user_karma_summary_table: String = config.getString("cassandra.user_karma_points_summary.table")
  val user_assessment_data_table: String = config.getString("cassandra.user_assessment_data.table")
  val course_batch_table: String = config.getString("cassandra.course_batch.table")

  val user_karma_coin_lookup_table: String = config.getString("cassandra.user_karma_coin_lookup.table")

  val user_karma_coin_wallet_table: String = config.getString("cassandra.user_karma_coin_wallet.table")
  val user_karma_coin_monthly_summary_table: String = config.getString("cassandra.user_karma_coin_monthly_summary.table")

  // Karma Coin transaction ledger - same `sunbird` keyspace, own table.
  val user_karma_coin_transactions_table: String = config.getString("cassandra.user_karma_coin_transactions.table")

  // Redis
  val cacheDbId: Int = if (config.hasPath("redis.database.karmaPointCache.id")) config.getInt("redis.database.karmaPointCache.id") else 0
  val metaRedisHost: String = config.getString("redis.host")
  val metaRedisPort: Int = config.getInt("redis.port")
  val karmaRedisTTLSeconds: Int = if (config.hasPath("redis.cache.ttl.seconds")) config.getInt("redis.cache.ttl.seconds") else 259200

  // External services
  val cbPlanV2Base: String = config.getString("service.cbplan.v2.basePath")
  val cbPlanV2ReadUser: String = cbPlanV2Base + "cbplan/v2/user/lookup"
  val cbEventReadUrl: String = config.getString("service.event.read")
  val userAccBlockedErrCode = "UOS_USRRED0006"

  // Karma point quotas - identical values to V1, no business-rule change
  val acbpQuotaKarmaPoints: Int = config.getInt("karmapoints.acbpQuotaKarmaPoints")
  val courseCompletionQuotaKarmaPoints: Int = config.getInt("karmapoints.courseCompletionQuotaKarmaPoints")
  val learningPathwayCompletionQuotaKarmaPoints: Int = config.getInt("karmapoints.learningPathwayCompletionQuotaKarmaPoints")
  val assessmentQuotaKarmaPoints: Int = config.getInt("karmapoints.assessmentQuotaKarmaPoints")
  val ratingQuotaKarmaPoints: Int = config.getInt("karmapoints.ratingQuotaKarmaPoints")
  val firstLoginQuotaKarmaPoints: Int = config.getInt("karmapoints.firstLoginQuotaKarmaPoints")
  val firstEnrolmentQuotaKarmaPoints: Int = config.getInt("karmapoints.firstEnrolmentQuotaKarmaPoints")
  val nonAcbpCourseQuota: Int = config.getInt("karmapoints.nonAcbpCourseQuota")
  val eventQuotaKarmaPoints: Int = config.getInt("karmapoints.eventQuotaKarmaPoints")
  val enableKarmaPointsCapping: Boolean = if (config.hasPath("karmapoints.enableCapping")) config.getBoolean("karmapoints.enableCapping") else true

  // Metrics enablement
  val metricsEnabled: Boolean = if (config.hasPath("metrics.enabled")) config.getBoolean("metrics.enabled") else true

  // Event type discriminator values routed by KarmaPointsProcessorFnV2
  val EVENT_TYPE_COURSE_COMPLETION = "COURSE_COMPLETION"
  val EVENT_TYPE_RATING = "RATING"
  val EVENT_TYPE_FIRST_ENROLMENT = "FIRST_ENROLMENT"
  val EVENT_TYPE_FIRST_LOGIN = "FIRST_LOGIN"
  val EVENT_TYPE_ACBP_CLAIM = "ACBP_CLAIM"
  val EVENT_TYPE_EVENT_ATTENDED = "EVENT_ATTENDED"
  val EVENT_TYPE_UNENROLMENT = "UNENROLMENT"

  val EVENT_TYPE_POINTS_CONVERSION = "POINTS_CONVERSION"
  val EVENT_TYPE_COINS_REDEMPTION = "COINS_REDEMPTION"

  val OPERATION_CREDIT = "CREDIT"
  val OPERATION_DEBIT = "DEBIT"
  val ACTION_TYPE_POINTS_REDEMPTION = "POINTS_REDEMPTION"

  val pointsConversionMonthlyLimit: Int =
    if (config.hasPath("karmaCoin.pointsConversion.monthlyLimit")) config.getInt("karmaCoin.pointsConversion.monthlyLimit") else 300

  val karmaCoinCacheTTLSeconds: Int =
    if (config.hasPath("karmaCoin.redis.cacheTtlSeconds")) config.getInt("karmaCoin.redis.cacheTtlSeconds") else 3600
  val karmaCoinRequestClaimTTLSeconds: Int =
    if (config.hasPath("karmaCoin.redis.requestClaimTtlSeconds")) config.getInt("karmaCoin.redis.requestClaimTtlSeconds") else 14400

  val pointsConversionDedupEnabled: Boolean =
    if (config.hasPath("karmaCoin.redis.pointsConversionDedupEnabled"))
      config.getBoolean("karmaCoin.redis.pointsConversionDedupEnabled")
    else
      true
  val coinsRedemptionDedupEnabled: Boolean =
    if (config.hasPath("karmaCoin.redis.coinsRedemptionDedupEnabled"))
      config.getBoolean("karmaCoin.redis.coinsRedemptionDedupEnabled")
    else
      true

  // Cassandra column / field constants (same DB schema as V1)
  val HIERARCHY = "hierarchy"
  val COURSE_ID = "courseId"
  val PRIMARY_CATEGORY = "primaryCategory"
  val courseCategory = "courseCategory"
  val name = "name"
  val identifier = "identifier"
  val USER_ID = "userid"
  val CREDIT_DATE = "credit_date"
  val CONTEXT_TYPE = "context_type"
  val OPERATION_TYPE = "operation_type"
  val CONTEXT_ID = "context_id"
  val ADD_INFO = "addinfo"
  val POINTS = "points"

  val DB_COLUMN_USER_KARMA_POINTS_KEY = "user_karma_points_key"
  val DB_COLUMN_USER_KARMA_COIN_KEY = "user_karma_coin_key"
  val DB_COLUMN_OPERATION_TYPE = "operation_type"
  val DB_COLUMN_USERID = "userid"
  val DB_COLUMN_CREDIT_DATE = "credit_date"
  val DB_COLUMN_CONTEXT_TYPE = "context_type"
  val DB_COLUMN_CONTEXT_ID = "context_id"
  val DB_COLUMN_ASSESSMENT_ID = "assessmentid"
  val DB_COLUMN_SUBMIT_ASSESSMENT_RESPONSE = "submitassessmentresponse"
  val DB_COLUMN_COURSE_ID = "courseid"
  val DB_COLUMN_BATCH_ID = "batchid"
  val DB_COLUMN_END_DATE = "end_date"

  val CHILDREN = "children"
  val COURSE_ASSESSMENT = "Course Assessment"
  val IDENTIFIER = "identifier"
  val RESULT = "result"
  val PASS = "pass"
  val OPERATION_TYPE_RATING = "RATING"
  val OPERATION_TYPE_FIRST_LOGIN = "FIRST_LOGIN"
  val OPERATION_TYPE_ENROLMENT = "FIRST_ENROLMENT"
  val OPERATION_COURSE_COMPLETION = "COURSE_COMPLETION"
  val OPERATION_LEARNING_PATHWAY_COMPLETION = "LEARNING_PATHWAY_COMPLETION"
  val OPERATION_TYPE_EVENT = "EVENT_ATTENDED"
  val CONTEXT_TYPE_EVENT = "EVENT"
  val ADDINFO_ASSESSMENT = "ASSESSMENT"
  val ADDINFO_ACBP = "ACBP"
  val ADDINFO_COURSENAME = "COURSENAME"
  val ADDINFO_ASSESSMENT_PASS = "ASSESSMENT_PASS"
  val ADDINFO_EVENTNAME = "EVENTNAME"
  val ADDINFO_UNENROLMENT = "UNENROLMENT"
  val ADDINFO_REENROLMENT = "REENROLMENT"

  val ID = "id"
  val SELF_REGISTRATION = "self_registration"
  val HEADER_CONTENT_TYPE_KEY = "Content-Type"
  val HEADER_CONTENT_TYPE_JSON = "application/json"
  val X_AUTHENTICATED_USER_ORGID = "x-authenticated-user-orgid"
  val X_AUTHENTICATED_USER_ID = "x-authenticated-userid"
  val END_DATE = "endDate"
  val END_TIME = "endTime"
  val CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA = "claimedNonACBPCourseKarmaQuota"
  val FORMATTED_MONTH = "formattedMonth"
  val TOTAL_POINTS = "total_points"
  val YYYY_PIPE_MM = "yyyy|MM"
  val EMPTY = ""
  val COURSE = "Course"
  val LEARNING_PATHWAY = "Learning Pathway"
  val ROOT_ORG_ID = "rootorgid"
  val LANGUAGE_MAP_v1 = "languageMapV1"
  val COMPLETED_LANGUAGE = "completedLanguage"
  val CONTENTS = "contents"
  val EVENT = "event"
  val NAME = "name"
  val PIPE = "|"

  val STATUS = "status"
  val STATUS_PROCESSING = "PROCESSING"
  val STATUS_FAILED = "FAILED"
  val STATUS_SUCCESS = "SUCCESS"

  val TOTAL_EARNED = "total_earned"
  val TOTAL_REDEEMED = "total_redeemed"
  val YEAR_MONTH = "year_month"
  val POINTS_CONVERTED = "points_converted"
  val UPDATED_ON = "updated_on"
  val YYYY_DASH_MM = "yyyy-MM"
  val CREATED_AT = "created_at"
  val DB_COLUMN_TRANSACTION_ID = "transaction_id"
  val TYPE = "type"
  val AMOUNT = "amount"
  val BALANCE_AFTER = "balance_after"
  val ACTION_TYPE = "action_type"

  val ADDINFO_ERROR_CODE = "errorCode"
  val ADDINFO_ERROR_MESSAGE = "errorMessage"
  val ADDINFO_TRANSACTION_ID = "transactionId"
  val ADDINFO_POINTS_CONVERTED = "pointsConverted"
  val ADDINFO_POINTS_USED = "pointsUsed"
  val ADDINFO_RATIO = "ratio"
  val RATIO_ONE_TO_ONE = "1:1"

  val ADDINFO_COURSE_NAME = "courseName"
  val ADDINFO_PROVIDER_NAME = "providerName"

  val ADDINFO_CREATED_AT = "createdAt"
  val ADDINFO_TARGET_TOTAL_EARNED = "targetTotalEarned"
  val ADDINFO_TARGET_TOTAL_REDEEMED = "targetTotalRedeemed"
  val ADDINFO_TARGET_YEAR_MONTH = "targetYearMonth"
  val ADDINFO_TARGET_POINTS_CONVERTED = "targetPointsConverted"
  val ERROR_CODE_CONVERSION_LIMIT_EXCEEDED = "CONVERSION_LIMIT_EXCEEDED"
  val ERROR_CODE_INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
  val TRANSACTION_ID_PREFIX = "KARMA_COIN"

  // Metric names
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbReadCount = "db-read-count"
  val dbUpdateCount = "db-update-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"
  val dataQualityErrorCount = "data-quality-error-count"
  val systemErrorCount = "system-error-count"
}
