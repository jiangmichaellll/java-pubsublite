package com.google.cloud.pubsublite.spark

import java.io.File

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.BeforeAndAfter

import scala.collection.mutable
import scala.collection.JavaConverters._

class PubSubLiteStreamSourceSuite extends SparkFunSuite with BeforeAndAfter {

  private val tempDir: File =
    new File(System.getProperty("java.io.tmpdir") + "/pubsub-lite-spark")

  private val conf = new SparkConf().setMaster("local[1]").setAppName("PubSubLiteStreamSourceSuite")
  protected val spark = SparkSession.builder().config(conf).getOrCreate()

  def createStreamingDataframe: (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = spark.sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tempDir.getAbsolutePath + "/checkpoint")

    val dataFrame: DataFrame =
      sqlContext.readStream.format("com.google.cloud.pubsublite.spark.PubSubLiteSourceProvider")
        .option("pubsublite.subscriber.usefake", "true")
        .load()
    (sqlContext, dataFrame)
  }

  before {
    tempDir.mkdirs()
  }

  test("run") {

    Console.println("[MJ] abc")

    Console.println(s"""${SparkSession.getActiveSession.get.sparkContext.defaultParallelism}""")

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe
    dataFrame.writeStream.format("console").start().awaitTermination(30000)

  }


}
