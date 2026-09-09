package org.sunbird.job.karmapoints.v2.exceptions

/**
 * Root of the V2 exception taxonomy. Every exception raised by handlers/storage
 * must be one of the two subclasses below so [[org.sunbird.job.karmapoints.v2.functions.KarmaPointsProcessorFnV2]]
 * can route it deterministically: DataQualityException -> failed-topic (skip, keep running),
 * SystemException -> rethrow (crash the task, Flink restarts and replays from checkpoint).
 */
sealed trait KarmaException extends Exception {
  def message: String

  def cause: Option[Throwable]
}

/**
 * A malformed/incomplete/business-invalid event. Not retryable by restarting the job -
 * the same bytes would fail again. Routed to the failed-topic side-output.
 */
abstract class DataQualityException(val message: String, val cause: Option[Throwable] = None)
  extends RuntimeException(message, cause.orNull) with KarmaException

/**
 * An infrastructure failure (Cassandra/Redis/HTTP down or exhausted retries). Retryable -
 * rethrown so Flink's restart strategy replays the event from the last checkpoint.
 */
abstract class SystemException(val message: String, val cause: Option[Throwable] = None)
  extends RuntimeException(message, cause.orNull) with KarmaException

// ---- Data quality exceptions ----

case class MissingEventTypeException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

case class InvalidUserIdException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

case class InvalidUserException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

/** Envelope's edata payload is missing or empty for an event type that requires it. */
case class MissingPayloadException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

/** eventType field is present but doesn't match any known handler. */
case class UnknownEventTypeException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

/** A payload field is present and non-empty but its value fails a business/contract constraint
 * (e.g. contextType/operation/actionType must equal a specific literal, pointsToConvert must be
 * > 0) - distinct from [[MissingPayloadException]], which is for absent/empty fields. */
case class InvalidPayloadException(override val message: String, override val cause: Option[Throwable] = None)
  extends DataQualityException(message, cause)

// ---- System exceptions ----

case class CassandraException(override val message: String, override val cause: Option[Throwable] = None)
  extends SystemException(message, cause)

case class RedisException(override val message: String, override val cause: Option[Throwable] = None)
  extends SystemException(message, cause)

/** External HTTP dependency (CB-Plan, event-service) timed out, errored, or returned an unexpected status. */
case class HttpServiceException(override val message: String, override val cause: Option[Throwable] = None)
  extends SystemException(message, cause)

/** Catch-all wrap for any exception a handler didn't recognize as data-quality - treated as infra/system. */
case class UnexpectedSystemException(override val message: String, override val cause: Option[Throwable] = None)
  extends SystemException(message, cause)
