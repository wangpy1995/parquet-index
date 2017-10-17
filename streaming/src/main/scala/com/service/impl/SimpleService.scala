package com.service.impl

import com.service.impl.SimpleService._
import com.service.{Service, ServiceComponent, Simple, SimpleComponent}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.SimpleStreamingContext
import org.apache.spark.utils.test.KafkaTestUtils

import scala.collection.mutable.ArrayBuffer

//@Component("simpleService")
trait SimpleService extends SimpleComponent with ServiceComponent {
  self: Simple with Service =>

  val tmp = ArrayBuffer.empty[String]

  def submitTask(sqlText: String): Unit = {
    arr.synchronized {
      arr += sql(sqlText).rdd
    }
  }

  def sendMessage(topic: String, id: Int, name: String, age: Int, cacheProducer: Boolean): Unit = {
    kafkaTestUtils.sendMessages(topic, Array(id + "\t" + name + "\t" + age))
    if (!cacheProducer)
      kafkaTestUtils.closeProducer(topic)
  }

  def sql(sqlText: String) = ssc.sql(sqlText)

  def get(): Seq[String] = {
    tmp ++= results
    if (results.nonEmpty)
      results.synchronized(results.clear())
    tmp
  }

  def startStream() = {
    ssc.start()
    new Thread(new Runnable {
      override def run(): Unit = {
        ssc.awaitTermination()
      }
    })
  }

  def stopAll() = ssc.stop(stopSparkContext = true, stopGracefully = true)
}

object SimpleService {
  private val sparkConf = new SparkConf().setAppName("simple").setMaster("local[*]")
  private val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val arr = ArrayBuffer.empty[RDD[Row]]
  val results = ArrayBuffer.empty[String]

  val ssc = new SimpleStreamingContext(ss, Seconds(10))

  private val kafkaTestUtils = new KafkaTestUtils
  kafkaTestUtils.setup()
  createKafkaTable("test")

  private def createKafkaTable(table: String) = {
    ss.sql(
      s"""
         |CREATE TEMPORARY VIEW $table(
         |id int,
         |name string,
         |age int
         |)
         |USING org.apache.spark.sql.execution.datasources.KafkaDatasource
         |OPTIONS (
         |  topic 'test',
         |  group.id '1',
         |  bootstrap.servers '${kafkaTestUtils.brokerAddress}',
         |  zookeeper.connect '${kafkaTestUtils.zkAddress}',
         |  key.serializer 'org.apache.kafka.common.serialization.StringSerializer',
         |  value.serializer 'org.apache.kafka.common.serialization.StringSerializer',
         |  key.deserializer 'org.apache.kafka.common.serialization.StringDeserializer',
         |  value.deserializer 'org.apache.kafka.common.serialization.StringDeserializer'
         |)
      """.stripMargin)
  }

}