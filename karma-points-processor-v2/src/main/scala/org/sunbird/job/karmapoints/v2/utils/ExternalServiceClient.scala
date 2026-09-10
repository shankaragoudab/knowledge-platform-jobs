package org.sunbird.job.karmapoints.v2.utils

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.karmapoints.v2.exceptions.HttpServiceException
import org.sunbird.job.util.{HttpUtil, ScalaJsonUtil}

import java.time.{LocalDate, LocalDateTime, OffsetTime, ZoneId}
import java.util.Date

/**
 * Wraps the two external HTTP dependencies shared by CourseCompletionHandler / ACBPClaimHandler
 * (CB-Plan ACBP lookup) and EventAttendedHandler (event-service read), factored out once instead
 * of tripled across handlers. Splits V1's conflated "any non-200/400 -> throw" into the V2 2-path
 * shape: blocked-user (400 + known error code) is a business skip, everything else (timeout, 5xx,
 * connection refused) is a [[SystemException]] so it triggers a job restart instead of a silent
 * false-negative business skip.
 */
class ExternalServiceClient(config: KarmaPointsV2Config, httpUtil: HttpUtil) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ExternalServiceClient])

  private def get(url: String, headers: Map[String, String])(implicit metrics: Metrics): Map[String, AnyRef] = {
    val response = try {
      httpUtil.get(url, headers)
    } catch {
      case ex: Exception => throw HttpServiceException(s"HTTP GET failed for $url", Some(ex))
    }
    if (response.status == 200) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse(config.RESULT, Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
    } else if (response.status == 400 && response.body.contains(config.userAccBlockedErrCode)) {
      metrics.incCounter(config.skippedEventCount)
      logger.warn(s"User blocked - treating as business skip for $url: ${response.status}")
      Map.empty[String, AnyRef]
    } else {
      throw HttpServiceException(s"Unexpected response from $url: status=${response.status}, body=${response.body}")
    }
  }

  /** Returns the ACBP expiry timestamp string for `courseId` if it's on the user's plan, else "". */
  def acbpExpiryForCourse(courseId: String, headers: Map[String, String])(implicit metrics: Metrics): String = {
    val response = get(config.cbPlanV2ReadUser, headers)
    response.getOrElse(config.CONTENTS, Map.empty[String, AnyRef]) match {
      case courses: Map[String @unchecked, AnyRef @unchecked] => courses.get(courseId).map(_.toString).getOrElse(config.EMPTY)
      case _ => config.EMPTY
    }
  }

  /** Returns whether `courseId` is present on the user's ACBP plan at all. */
  def isCourseOnACBPPlan(courseId: String, headers: Map[String, String])(implicit metrics: Metrics): Boolean =
    acbpExpiryForCourse(courseId, headers).nonEmpty

  def eventNameAndEndDate(eventId: String, headers: Map[String, String])(implicit metrics: Metrics): (String, Date) = {
    val response = get(config.cbEventReadUrl + eventId, headers)
    val event = response.getOrElse(config.EVENT, Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
    val endDate = event.getOrElse(config.END_DATE, "").asInstanceOf[String]
    val endTime = event.getOrElse(config.END_TIME, "").asInstanceOf[String]
    if (endDate.isEmpty || endTime.isEmpty) {
      logger.warn(s"Either endDate or endTime missing for eventId=$eventId")
    }
    val localDate = LocalDate.parse(endDate)
    val offsetTime = OffsetTime.parse(endTime)
    val localDateTime = LocalDateTime.of(localDate, offsetTime.toLocalTime)
    val date = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant)
    (event.getOrElse(config.NAME, "").asInstanceOf[String], date)
  }
}
