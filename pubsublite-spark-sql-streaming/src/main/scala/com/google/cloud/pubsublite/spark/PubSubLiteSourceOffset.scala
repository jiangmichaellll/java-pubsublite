package com.google.cloud.pubsublite.spark

import com.google.cloud.pubsublite.{Offset, Partition}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => SparkOffset}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable

/**
 * A [[SparkOffset]] for PubSub Lite. It tracks the offsets of all partitions for a topic.
 */
class PubSubLiteSourceOffset(partitionToOffsets: Map[Partition, Offset]) extends SparkOffset {

  private implicit val formats = Serialization.formats(NoTypeHints)
  private implicit object partitionOrdering extends Ordering[Partition] {
    override def compare(x: Partition, y: Partition): Int = x.value() compare y.value()
  }

  def getMap(): Map[Partition, Offset] = {
    partitionToOffsets
  }

  /**
   * Write per-partition offsets as json string
   */
  def toJson: String = {
    val result = mutable.HashMap[Long, Long]()
    val partitions = partitionToOffsets.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { p =>
      val off = partitionToOffsets(p)
      result.update(p.value(), off.value())
    }
    Serialization.write(result)
  }

  override def json(): String = {
    toJson
  }
}

object PubSubLiteSourceOffset {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def fromJson(json: String): PubSubLiteSourceOffset = {
    val result: mutable.HashMap[Long, Long] = Serialization.read(json)
    new PubSubLiteSourceOffset(result.map {
      case (k, v) => (Partition.of(k), Offset.of(v))
    }.toMap)
  }

  def combine(o1: PubSubLiteSourceOffset, o2: PubSubLiteSourceOffset): PubSubLiteSourceOffset = {
    assert(o1 != null || o2 != null)
    if (o1 == null) {
      return o2
    }
    if (o2 == null) {
      return o1
    }
    val allPartitions = o1.getMap().keySet.union(o2.getMap().keySet)
    val result = new mutable.HashMap[Partition, Offset]
    allPartitions.foreach( p =>
      if (o1.getMap().contains(p) && o2.getMap().contains(p)) {
        result.update(p, Offset.of(math.max(o1.getMap().get(p).get.value(), o2.getMap().get(p).get.value())))
      } else if (o1.getMap().contains(p)) {
        result.update(p, Offset.of(o1.getMap().get(p).get.value()))
      } else {
        result.update(p, Offset.of(o2.getMap().get(p).get.value()))
      }
    )
    new PubSubLiteSourceOffset(result.toMap)
  }
}
