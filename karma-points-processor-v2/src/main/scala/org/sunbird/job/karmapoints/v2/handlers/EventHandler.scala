package org.sunbird.job.karmapoints.v2.handlers

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.domain.UnifiedEvent
import org.sunbird.job.karmapoints.v2.exceptions.{DataQualityException, SystemException, UnexpectedSystemException}

/**
 * Shared try/catch shape for all 7 event handlers (Step 6, point 2 of every handler): business
 * logic lives in `doHandle`, and this trait enforces that whatever it throws lands in exactly one
 * of the two V2 error-handling buckets before reaching [[org.sunbird.job.karmapoints.v2.functions.KarmaPointsProcessorFnV2]] -
 * DataQualityException/SystemException pass through unchanged, anything else gets wrapped as a
 * SystemException so an unanticipated bug fails safe (job restart) rather than silently routing
 * to the failed-topic as if it were a business-rule violation.
 */
trait EventHandler {

  private val logger = LoggerFactory.getLogger(getClass)

  protected def doHandle(event: UnifiedEvent)(implicit metrics: Metrics): Unit

  final def handle(event: UnifiedEvent)(implicit metrics: Metrics): Unit = {
    try {
      doHandle(event)
    } catch {
      case ex: DataQualityException => throw ex
      case ex: SystemException => throw ex
      case ex: Exception =>
        logger.error(s"Unexpected exception in ${getClass.getSimpleName} for userId=${event.userId}", ex)
        throw UnexpectedSystemException(s"Unexpected error in ${getClass.getSimpleName}: ${ex.getMessage}", Some(ex))
    }
  }
}
