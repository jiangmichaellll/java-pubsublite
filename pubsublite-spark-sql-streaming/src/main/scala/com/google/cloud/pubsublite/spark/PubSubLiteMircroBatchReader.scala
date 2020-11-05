package com.google.cloud.pubsublite.spark

import java.util
import java.util.Optional
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.cloud.pubsublite.{Partition, SubscriptionPath, spark, Offset => PubSubLiteOffset}
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings
import com.google.cloud.pubsublite.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UTF8StringBuilder
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, StructType}

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable

class PubSubLiteMircroBatchReader(consumer: Consumer[Array[Byte], Array[Byte]]) extends MicroBatchReader
  with Logging {

  private var cache = new PubSubLiteMessageCache(consumer)

  private var offsetLock = new ReentrantReadWriteLock()
  // (start, end]
  private var startOffset: Offset = _
  private var endOffset: Offset = _

  def printStartEndNoLock(location: String): Unit = {
    Console.println(location)
    if (startOffset != null) {
      Console.println(s"""start: ${startOffset.json()}""")
    }
    if (endOffset != null) {
      Console.println(s"""end: ${endOffset.json()}""")
    }
    if (cache.getLatestAvailableOffsets != null) {
      Console.println(s"""latest: ${cache.getLatestAvailableOffsets.json()}""")
    }
    Console.println("=======")
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    offsetLock.writeLock().lock()
    try {
      startOffset = Option(start.orElse(null)) match {
        case Some(o) => o
        case None =>
          val committedOffsets = consumer.committed(consumer.assignment()).map {
            case (key, value) => (Partition.of(key.partition()), PubSubLiteOffset.of(value.offset()))
          }
          new PubSubLiteSourceOffset(committedOffsets.toMap)
      }
      endOffset = end.orElse(cache.getLatestAvailableOffsets)

      printStartEndNoLock("setOffsetRange")
    } finally {
      offsetLock.writeLock().unlock()
    }
  }

  override def getStartOffset: Offset = {
    offsetLock.readLock().lock()
    try {

      printStartEndNoLock("getStart")

      Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
    } finally {
      offsetLock.readLock().unlock()
    }
  }

  override def getEndOffset: Offset = {
    offsetLock.readLock().lock()
    try {
//      Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))

      printStartEndNoLock("getEnd")

      endOffset
    } finally {
      offsetLock.readLock().unlock()
    }
  }

  override def deserializeOffset(json: String): Offset = PubSubLiteSourceOffset.fromJson(json)

  override def commit(`end`: Offset): Unit = cache.commit(end.asInstanceOf[PubSubLiteSourceOffset])

  override def readSchema(): StructType = PubSubLiteConstants.DEFAULT_SCHEMA

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    offsetLock.readLock().lock()

    var range: Map[Partition, (PubSubLiteOffset, PubSubLiteOffset)] = null
    try {
      assert(startOffset != null && endOffset != null,
        "start offset and end offset should already be set before create read tasks.")
      range = calculateRange(startOffset, endOffset)
    } finally {
      offsetLock.readLock().unlock()
    }

    range.keySet.map { p =>
      val messagesPerPartition = cache.get(p, range.get(p).get)
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
          new InputPartitionReader[InternalRow] {
            private var idx = -1

            override def next(): Boolean = {
              idx += 1
              idx < messagesPerPartition.length
            }

            override def get(): InternalRow = {
              val msg = messagesPerPartition(idx)
              val dataBuilder = new UTF8StringBuilder()
              dataBuilder.append(msg.data().toStringUtf8)
              InternalRow(
                p.value(),
                dataBuilder.build(),
                msg.eventTime().get()
              )
            }

            override def close(): Unit = {}
          }
      }
    }.toList.asJava
  }

  override def stop(): Unit = {
    consumer.close()
  }

  def calculateRange(startOffset: Offset, endOffset: Offset): Map[Partition, (PubSubLiteOffset, PubSubLiteOffset)] = {
    val range = new mutable.HashMap[Partition, (PubSubLiteOffset, PubSubLiteOffset)]

    assert(startOffset.isInstanceOf[PubSubLiteSourceOffset], "wrong typed offset")
    assert(endOffset.isInstanceOf[PubSubLiteSourceOffset], "wrong typed offset")
    val start = startOffset.asInstanceOf[PubSubLiteSourceOffset]
    val end = endOffset.asInstanceOf[PubSubLiteSourceOffset]
    for (k <- end.getMap().keySet) {
      if (!start.getMap().contains(k)) {
        throw new IllegalStateException("startoffset doesn't have key that endoffset has.")
      }
      range.update(k, (PubSubLiteOffset.of(start.getMap().get(k).get.value()),
        PubSubLiteOffset.of(end.getMap().get(k).get.value())))
    }
    range.toMap
  }
}
