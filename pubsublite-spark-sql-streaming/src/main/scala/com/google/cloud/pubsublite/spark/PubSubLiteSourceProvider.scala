package com.google.cloud.pubsublite.spark

import java.util.Optional

import com.google.cloud.pubsublite.SubscriptionPath
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings
import com.google.cloud.pubsublite.kafka.ConsumerSettings
import com.google.common.collect.ImmutableMap
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.collection.JavaConverters.mapAsScalaMapConverter

class PubSubLiteSourceProvider extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister
  with Logging {


  override def shortName(): String = "pubsublite"

  override def createMicroBatchReader(schema: Optional[StructType], checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    val parameters = options.asMap().asScala.toMap

    if (schema.isPresent) {
      throw new IllegalArgumentException("PubSubLite spark sql streaming doesn't support custom schema.")
    }

    val consumer = parameters.getOrElse("pubsublite.subscriber.usefake", "false").toBoolean match {
      case true => new FakeConsumer(ImmutableMap.of("a", "b"))
      case false => {
        val subscriptionPath = parameters.get("pubsublite.subscription") match {
          case Some(s) => SubscriptionPath.parse(s)
          case None => throw new IllegalArgumentException("pubsublite.subscription params required.")
        }
        val bytesOutstanding = parameters.get("pubsublite.subscriber.flowcontrol.bytesoutstanding") match {
          case Some(s) => Integer.parseInt(s)
          case None => PubSubLiteConstants.DEFAULT_BYTES_OUTSTANDING
        }
        val messagesOutstanding = parameters.get("pubsublite.subscriber.flowcontrol.messagesoutstanding") match {
          case Some(s) => Integer.parseInt(s)
          case None => PubSubLiteConstants.DEFAULT_MESSAGES_OUTSTANDING
        }
        val flowControlSettings = FlowControlSettings.builder()
          .setBytesOutstanding(bytesOutstanding)
          .setMessagesOutstanding(messagesOutstanding)
          .build()

        ConsumerSettings.newBuilder()
          .setAutocommit(false)
          .setPerPartitionFlowControlSettings(flowControlSettings)
          .setSubscriptionPath(subscriptionPath)
          .build()
          .instantiate()
      }
    }

    new PubSubLiteMircroBatchReader(consumer)
  }
}