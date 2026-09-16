package org.sunbird.job.karmapoints.v2.utils

import org.sunbird.job.Metrics
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config

import java.util.concurrent.atomic.{AtomicLong, LongAdder}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.JavaConverters._

/**
 * Observability wrapper around jobs-core's [[Metrics]] (which only supports flat named counters
 * gauged back to zero every metrics interval). Adds per-event-type and per-exception-type
 * breakdowns plus a latency histogram/throughput gauge, none of which the base `Metrics` case
 * class provides. `incCounter` still delegates to the base counters so existing metric names
 * (`config.totalEventsCount` etc.) keep flowing into the standard Flink metric group.
 */
class KarmaMetrics(config: KarmaPointsV2Config, base: Metrics) {

  private val eventTypeCounters = new ConcurrentHashMap[String, AtomicLong]()
  private val dataQualityErrorCounters = new ConcurrentHashMap[String, AtomicLong]()
  private val systemErrorCounters = new ConcurrentHashMap[String, AtomicLong]()
  private val latencySamplesMs = new ConcurrentLinkedQueue[Long]()
  private val maxLatencySamples = 1000
  private val processedSinceStart = new LongAdder()
  private val windowStartMs = System.currentTimeMillis()

  def incCounter(metric: String): Unit = base.incCounter(metric)

  def incEventTypeCounter(eventType: String): Unit = {
    eventTypeCounters.computeIfAbsent(eventType, _ => new AtomicLong(0)).incrementAndGet()
    processedSinceStart.increment()
  }

  def incDataQualityError(exceptionName: String): Unit = {
    base.incCounter(config.dataQualityErrorCount)
    dataQualityErrorCounters.computeIfAbsent(exceptionName, _ => new AtomicLong(0)).incrementAndGet()
  }

  def incSystemError(exceptionName: String): Unit = {
    base.incCounter(config.systemErrorCount)
    systemErrorCounters.computeIfAbsent(exceptionName, _ => new AtomicLong(0)).incrementAndGet()
  }

  /** Record one event's end-to-end processing latency, given its start time from System.nanoTime(). */
  def recordLatency(startNanos: Long): Unit = {
    val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
    latencySamplesMs.add(elapsedMs)
    while (latencySamplesMs.size() > maxLatencySamples) latencySamplesMs.poll()
  }

  /** p in [0.0, 1.0], e.g. 0.99 for p99 latency in ms over the current in-memory sample window. */
  def latencyPercentileMs(p: Double): Long = {
    val snapshot = latencySamplesMs.iterator().asScala.toArray.sorted
    if (snapshot.isEmpty) 0L
    else snapshot(math.min(snapshot.length - 1, (snapshot.length * p).toInt))
  }

  /** Throughput gauge: events processed per second since this metrics instance was created. */
  def throughputPerSecond: Double = {
    val elapsedSec = math.max(1L, (System.currentTimeMillis() - windowStartMs) / 1000L)
    processedSinceStart.sum().toDouble / elapsedSec
  }

  def eventTypeCounts: Map[String, Long] = eventTypeCounters.asScala.mapValues(_.get()).toMap

  def dataQualityErrorCounts: Map[String, Long] = dataQualityErrorCounters.asScala.mapValues(_.get()).toMap

  def systemErrorCounts: Map[String, Long] = systemErrorCounters.asScala.mapValues(_.get()).toMap
}
