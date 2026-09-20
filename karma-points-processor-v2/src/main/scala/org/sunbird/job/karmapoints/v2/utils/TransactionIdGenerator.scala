package org.sunbird.job.karmapoints.v2.utils

import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config

import java.security.SecureRandom

/**
 * Formats the `TXN-<12 uppercase alphanumeric characters>` transaction id - the SOLE Karma Coin
 * transaction id generator, used by POINTS_CONVERSION, COINS_REDEMPTION and COINS_REAWARD (all
 * insert into `user_karma_coin_transactions` under this id). Generated once per plan and
 * persisted; a replay reuses the id already frozen into the recovered plan rather than calling
 * this again, which is what keeps the transaction insert idempotent.
 */
private[v2] object TransactionIdGenerator {

  private val secureRandom = new SecureRandom()

  private[v2] def generate(config: KarmaPointsV2Config): String = {
    val alphabet = config.TRANSACTION_ID_ALPHABET
    val suffix = (1 to config.TRANSACTION_ID_LENGTH).map(_ => alphabet.charAt(secureRandom.nextInt(alphabet.length))).mkString
    s"${config.TRANSACTION_ID_PREFIX}-$suffix"
  }
}
