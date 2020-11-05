package com.google.cloud.pubsublite.spark

import java.time.Duration
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.cloud.pubsublite.kafka.ConsumerSettings
import com.google.cloud.pubsublite.{Message, Offset, Partition}
import com.google.common.util.concurrent.Striped
import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.spark.internal.Logging
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

import scala.collection.JavaConverters.mapAsJavaMap
import scala.collection.mutable

/**
 * A message cache that holds a subscriber and constantly refills the cache by pulling from
 * the subscriber.
 */
class PubSubLiteMessageCache(consumer: Consumer[Array[Byte], Array[Byte]]) extends Logging {

  private val MAX_PARTITION_COUNT = 10000

  private val lock = Striped.readWriteLock(MAX_PARTITION_COUNT)
  private val messages = new mutable.HashMap[Partition, mutable.HashMap[Offset, Message]]()

  private val offsetLock = new ReentrantReadWriteLock()
  private var latestAvailableOffsets: PubSubLiteSourceOffset = _

  // TODO(jiangmichael): change
//  private val topic = consumer.listTopics().keySet().toArray().head.asInstanceOf[String]
  private val topic = "test-topic"

  private val executor = new ScheduledThreadPoolExecutor(1)
  private val pullTask = executor.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val newMaxOffsets = new mutable.HashMap[Partition, Offset]
      consumer.poll(Duration.ofMillis(100)).forEach { r =>
        // TODO(jiangmichael): batch insert by partition
        insert(Partition.of(r.partition()), Offset.of(r.offset()), toMessage(r))
        val v = newMaxOffsets.get(Partition.of(r.partition()))
        v match {
          case Some(o) => if (r.offset() > o.value()) {
            newMaxOffsets.update(Partition.of(r.partition()), Offset.of(r.offset()))
          }
          case None =>
            newMaxOffsets.update(Partition.of(r.partition()), Offset.of(r.offset()))
        }
      }
      offsetLock.writeLock().lock()
      try {
        latestAvailableOffsets = PubSubLiteSourceOffset.combine(latestAvailableOffsets,
          new PubSubLiteSourceOffset(newMaxOffsets.toMap))
        if (latestAvailableOffsets != null) {
          Console.println(s"""latest available: ${latestAvailableOffsets.toJson}""")
          Console.println("=======")
        }
      } finally {
        offsetLock.writeLock().unlock()
      }
    }
  }, 100, 100, TimeUnit.MILLISECONDS)

  def toMessage(record: ConsumerRecord[Array[Byte], Array[Byte]]): Message = {
    val builder = Message.builder
      .setKey(ByteString.copyFrom(record.key))
      .setData(ByteString.copyFrom(record.value))
    builder.setEventTime(Timestamps.fromMillis(record.timestamp))
    builder.build()
  }

  def getLatestAvailableOffsets: PubSubLiteSourceOffset = {
    offsetLock.readLock().lock()
    try {
      latestAvailableOffsets
    } finally {
      offsetLock.readLock().unlock()
    }
  }

  def commit(offset: PubSubLiteSourceOffset) = {
    val commitMap: Map[KafkaTopicPartition, OffsetAndMetadata] = offset.getMap().map {
      case (k, v) => (
        new KafkaTopicPartition(topic, k.value().toInt),
        new OffsetAndMetadata(v.value()))
    }
    val offsetCommitCallback: OffsetCommitCallback = (offsets, exception) =>
      if (exception != null) {
        throw exception
      }
      // TODO(jiangmichael): delete data pre-offsets from cache.
    consumer.commitAsync(mapAsJavaMap(commitMap), offsetCommitCallback)
  }

  def insert(partition: Partition, offset: Offset, message: Message) = {
    validatePartition(partition)
    lock.getAt(partition.value().toInt).writeLock().lock()
    try {
      messages.getOrElseUpdate(partition, new mutable.HashMap[Offset, Message])
        .update(offset, message)
    } finally {
      lock.getAt(partition.value().toInt).writeLock().unlock()
    }
  }

  def validatePartition(partition: Partition) = {
    if (partition.value() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
        s"""PubSub Lite spark connector is not able to
           | handle topic with more than ${Integer.MAX_VALUE}
           |  partitions. Partition: ${partition.value()}""")
    }
  }

  def get(partition: Partition, range: (Offset, Offset)): Array[Message] = {
    validatePartition(partition)
    lock.getAt(partition.value().toInt).readLock().lock()
    try {
      messages.get(partition) match {
        case Some(map) => map.toMap
          .filterKeys(k =>k.value() >= range._1.value() && k.value() <= range._2.value())
          .values.toArray
        case _ => Array.empty
      }
    } finally {
      lock.getAt(partition.value().toInt).readLock().unlock()
    }
  }
}
