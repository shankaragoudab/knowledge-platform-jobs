package org.sunbird.job.karmapoints.v2.storage

import org.slf4j.LoggerFactory
import org.sunbird.job.cache.DataCache
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.util.JSONUtil
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisException}

/**
 * Thin wrapper over jobs-core's [[DataCache]] - same shared connection/DB index for every key this
 * class touches: Karma Points' `user:karmaPoints:<userId>` (a write-through mirror of the Cassandra
 * summary total, same as V1), Karma Coin's `user:karmaCoins:<userId>` (a write-through mirror of the
 * Cassandra wallet), and Karma Coin's request-level dedup claim keyed by `userId|contextType|contextId`
 * (a first-level, best-effort duplicate filter in front of Cassandra). Redis is never read for
 * business decisions here (V1 never did either) and is never the authoritative claim (the dedup key
 * is a fast-path optimization, not a substitute for `user_karma_coin_lookup`), so failures are
 * best-effort: logged and swallowed (or failed open), never escalated to a job restart.
 */
class RedisUtil(dataCache: DataCache, config: KarmaPointsV2Config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RedisUtil])

  private def keyFor(userId: String): String = s"user:karmaPoints:$userId"

  private def karmaCoinKeyFor(userId: String): String = s"user:karmaCoins:$userId"

  /** Mirrors the user's new total to Redis. Best-effort - a Redis outage must not fail the event. */
  def setUserKarmaPoints(userId: String, totalPoints: Int): Unit = {
    try {
      dataCache.setWithRetry(keyFor(userId), totalPoints.toString)
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        logger.error(s"Failed to mirror karma points to Redis for userId=$userId (best-effort, not fatal)", ex)
      case ex: Exception =>
        logger.error(s"Unexpected error mirroring karma points to Redis for userId=$userId (best-effort, not fatal)", ex)
    }
  }

  /** Reads the cached total. Returns 0 on a cache miss or on any Redis failure (best-effort). */
  def getUserKarmaPoints(userId: String): Int = {
    try {
      val value = dataCache.getStringValue(keyFor(userId))
      if (value != null && value.nonEmpty) value.toInt else 0
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read karma points from Redis for userId=$userId, defaulting to 0", ex)
        0
    }
  }

  /**
   * Mirrors the user's Karma Coin wallet to Redis after a successful POINTS_CONVERSION, with a
   * 3600s TTL. Best-effort, same shape as [[setUserKarmaPoints]] above - a Redis outage must not
   * fail the event; Cassandra remains the source of truth.
   */
  def setKarmaCoinWallet(userId: String, totalEarned: Int, totalRedeemed: Int, yearMonth: String, convertedThisMonth: Int): Unit = {
    try {
      val value = new java.util.HashMap[String, Any]()
      value.put("totalEarned", totalEarned)
      value.put("totalRedeemed", totalRedeemed)
      value.put("yearMonth", yearMonth)
      value.put("convertedThisMonth", convertedThisMonth)
      dataCache.set(karmaCoinKeyFor(userId), JSONUtil.serialize(value), config.karmaCoinCacheTTLSeconds)
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        logger.error(s"Failed to mirror karma coin wallet to Redis for userId=$userId (best-effort, not fatal)", ex)
      case ex: Exception =>
        logger.error(s"Unexpected error mirroring karma coin wallet to Redis for userId=$userId (best-effort, not fatal)", ex)
    }
  }

  /**
   * First-level (fast, best-effort) request dedup: atomically claims `requestKey`
   * (`userId|contextType|contextId`) for ~4 hours via Redis `SET NX EX`, storing the complete
   * Kafka event JSON as the value. Cassandra's own lookup claim (`user_karma_coin_lookup`) remains
   * the permanent, authoritative record - this only exists to let an obvious short-term-duplicate
   * redelivery skip Cassandra entirely. The key deliberately outlives the Kafka checkpoint/commit
   * (never deleted on success) - only the TTL retires it.
   *
   * On a genuine Redis failure (not "key already exists", an actual exception), this fails OPEN -
   * returns true (claimed) so processing falls through to Cassandra, exactly like every other
   * best-effort Redis path in this class: a Redis outage must never block or drop an event.
   *
   * @return true if this call claimed the key (proceed with processing); false if the key already
   *         existed (treat as a duplicate and skip, per the confirmed design - Cassandra is not
   *         consulted in that case).
   */
  def claimKarmaCoinDedup(requestKey: String, eventJson: String): Boolean = {
    try {
      dataCache.setIfAbsentWithTTL(requestKey, eventJson, config.karmaCoinRequestClaimTTLSeconds)
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        logger.error(s"Failed to claim karma coin request in Redis for requestKey=$requestKey " +
          s"(best-effort, falling through to Cassandra)", ex)
        true
      case ex: Exception =>
        logger.error(s"Unexpected error claiming karma coin request in Redis for requestKey=$requestKey " +
          s"(best-effort, falling through to Cassandra)", ex)
        true
    }
  }

  /**
   * Releases a first-level request-dedup claim made by [[claimKarmaCoinDedup]] - called only when
   * the caller's processing failed AFTER claiming (before Cassandra's own lookup reached SUCCESS),
   * so a subsequent redelivery of the same event (e.g. a Flink checkpoint replay following a
   * `SystemException`) isn't falsely short-circuited by a stale claim from an attempt that never
   * finished. Best-effort, same fail-safe shape as every other method in this class - a Redis
   * outage here must not mask the original exception the caller is already propagating. Reuses
   * jobs-core's existing `DataCache.delWithRetry` - no new Redis primitive.
   */
  def releaseKarmaCoinRequestClaim(requestKey: String): Unit = {
    try {
      dataCache.delWithRetry(requestKey)
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        logger.error(s"Failed to release karma coin request claim in Redis for requestKey=$requestKey (best-effort, not fatal)", ex)
      case ex: Exception =>
        logger.error(s"Unexpected error releasing karma coin request claim in Redis for requestKey=$requestKey (best-effort, not fatal)", ex)
    }
  }

  def close(): Unit = dataCache.close()
}
