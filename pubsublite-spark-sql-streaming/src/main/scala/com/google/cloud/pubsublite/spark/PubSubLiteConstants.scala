package com.google.cloud.pubsublite.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object PubSubLiteConstants {
  val DEFAULT_BYTES_OUTSTANDING = 10000L
  val DEFAULT_MESSAGES_OUTSTANDING = 100L
  val DEFAULT_SCHEMA = StructType(
    StructField("partition", IntegerType, false) ::
    StructField("value", StringType, false) ::
    StructField("timestamp", TimestampType, false) :: Nil)
}
