package com.service.impl

import com.service.Simple
import com.service.impl.SimpleImpl._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{SimpleDStream, SimpleStreamingContext}
import org.apache.spark.utils.test.KafkaTestUtils

import scala.collection.mutable.ArrayBuffer

//@Component("simpleService")
trait SimpleImpl extends Simple {

  val tmp = ArrayBuffer.empty[String]

  def submitTask(sqlText: String) = {
    arr.synchronized {
      arr += sql(sqlText).rdd
    }
  }

  def sendMessage(topic: String, id: Int, name: String, age: Int): Unit = {
    kafkaTestUtils.sendMessages(topic, Array(id + "\t" + name + "\t" + age))
  }

  def sql(sqlText: String) = ssc.sql(sqlText)

  def getResults: Array[String] = {
    tmp ++= results
    if (results.nonEmpty) results.clear()
    val res = tmp.toArray
    tmp.clear()
    res
  }
}

object SimpleImpl {
  private val sparkConf = new SparkConf().setAppName("simple").setMaster("local[*]")
  private val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val arr = ArrayBuffer.empty[RDD[Row]]
  val results = ArrayBuffer.empty[String]

  val ssc = new SimpleStreamingContext(ss, Seconds(10))

  private val kafkaTestUtils = new KafkaTestUtils
  kafkaTestUtils.setup()
  createKafkaTable("test")

  val stream = new SimpleDStream(ssc, arr)
  stream.foreachRDD(_.filter(_ != null).foreachPartition {
    _.foreach { row =>
      results += row.toString()
    }
  })
  ssc.start()
  new Thread(new Runnable {
    override def run(): Unit = {
      ssc.awaitTermination()
    }
  })

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