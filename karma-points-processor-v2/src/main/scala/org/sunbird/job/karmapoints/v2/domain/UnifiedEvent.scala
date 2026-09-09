package org.sunbird.job.karmapoints.v2.domain

import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

/**
 * Common envelope V2 producers publish to the single `karma-points-unified-v2` topic, replacing
 * the 7 ad-hoc payload shapes V1 consumed. Shape:
 * {
 * "eid": "BE_JOB_REQUEST", "ets": 1700000000000, "mid": "...",
 * "eventType": "COURSE_COMPLETION" | "RATING" | "FIRST_ENROLMENT" | "FIRST_LOGIN"
 * | "ACBP_CLAIM" | "EVENT_ATTENDED" | "UNENROLMENT",
 * "userId": "<uuid>",
 * "edata": { ...fields specific to eventType, e.g. courseId/activityId/eventId/batchId... }
 * }
 * `eventType` and `userId` are promoted to top-level fields (unlike V1's per-topic conventions)
 * because they are the two fields every handler and the keyBy partitioning need up front.
 */
class UnifiedEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def eventType: String = readOrDefault[String]("eventType", "")

  def userId: String = readOrDefault[String]("userId", "")

  def ets: Long = readOrDefault[Long]("ets", 0L)

  /**
   * V2 wrapper support: `data`/`edata` normally arrive as `java.util.Map` via Jackson, but nested
   * `Any`-typed fields are deserialized by jackson-module-scala's untyped-object handling as
   * `scala.collection.Map` instead - this accepts either shape rather than assuming one.
   */
  private def asMap(raw: Any): Map[String, Any] = raw match {
    case null => Map.empty[String, Any]
    case m: java.util.Map[String @unchecked, Any @unchecked] => m.asScala.toMap
    case m: scala.collection.Map[String @unchecked, Any @unchecked] => m.toMap
    case _ => Map.empty[String, Any]
  }

  def edata: Map[String, Any] = asMap(readOrDefault[Any]("edata", null))

  def edataString(key: String, default: String = ""): String = edata.get(key) match {
    case Some(v) if v != null => v.toString
    case _ => default
  }

  def edataBoolean(key: String, default: Boolean = false): Boolean = edata.get(key) match {
    case Some(v: Boolean) => v
    case Some(v) if v != null => v.toString.toBoolean
    case _ => default
  }

  def edataLong(key: String, default: Long = 0L): Long = edata.get(key) match {
    case Some(v) if v != null => v.toString.toLong
    case _ => default
  }

  /**
   * COURSE_COMPLETION support: V1's `edata.userIds` is a JSON array with exactly one element
   * (unlike UNENROLMENT's identically-named but single-String `edata.userIds`) - this reads the
   * first element without assuming any other accessor's shape.
   */
  def edataStringArrayFirst(key: String, default: String = ""): String = edata.get(key) match {
    case Some(list: java.util.List[_]) if !list.isEmpty => Option(list.get(0)).map(_.toString).getOrElse(default)
    case Some(list: scala.collection.Seq[_]) if list.nonEmpty => Option(list.head).map(_.toString).getOrElse(default)
    case _ => default
  }

  /**
   * V2 wrapper support: some producers send `{eventType, data: {...original V1 payload...}, version}`
   * instead of the `edata` shape above. `data` exposes that inner V1 payload untouched - no key
   * renaming/casing/flattening - so a handler can read it using the original V1 field names.
   */
  def data: Map[String, Any] = asMap(readOrDefault[Any]("data", null))

  def dataString(key: String, default: String = ""): String = data.get(key) match {
    case Some(v) if v != null => v.toString
    case _ => default
  }

  def dataLong(key: String, default: Long = 0L): Long = data.get(key) match {
    case Some(v) if v != null => v.toString.toLong
    case _ => default
  }

  /**
   * V2 FIRST_ENROLMENT wrapper support: that payload nests its fields one level deeper than
   * RATING's - under `data.edata.*` - so this reads the nested object without any renaming or
   * flattening, same original V1 field names as `edata`/`edataString` above.
   */
  def dataEdata: Map[String, Any] = asMap(data.get("edata").orNull)

  def dataEdataString(key: String, default: String = ""): String = dataEdata.get(key) match {
    case Some(v) if v != null => v.toString
    case _ => default
  }

  def dataEdataBoolean(key: String, default: Boolean = false): Boolean = dataEdata.get(key) match {
    case Some(v: Boolean) => v
    case Some(v) if v != null => v.toString.toBoolean
    case _ => default
  }
}
