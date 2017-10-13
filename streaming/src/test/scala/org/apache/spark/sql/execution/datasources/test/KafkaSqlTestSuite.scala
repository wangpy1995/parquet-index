package org.apache.spark.sql.execution.datasources.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.SimpleDStream
import org.scalatest.FunSuite
import service.SimpleController

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

class KafkaSqlTestSuite extends FunSuite {

  private lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
  private lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private val kafkaTestUtils = new KafkaTestUtils
  kafkaTestUtils.setup()

  //  kafkaTestUtils.sendMessages("test",Array("0\t'a'\t1"))
  def createKafkaTempTable(table: String) = {
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

  test("kafka_sql") {
    createKafkaTempTable("KAFKA_STU")
    ss.sql("select * from KAFKA_STU").show()
    ss.sql("INSERT INTO TABLE KAFKA_STU VALUES(1,'wpy',25)")
    ss.sql("select name from KAFKA_STU").show()
    StdIn.readLine()
  }
}

object Test {
  private lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
  private lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private val kafkaTestUtils = new KafkaTestUtils
  kafkaTestUtils.setup()

  //  kafkaTestUtils.sendMessages("test",Array("0\t'a'\t1"))
  def createKafkaTempTable(table: String) = {
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

  def main(args: Array[String]): Unit = {
    import service.SimpleService._
    createKafkaTempTable("KAFKA_STU")
    val controller = new SimpleController
    var i = 0
    val rdds = controller.search(15, "KAFKA_STU", "id,name,age")
    val arr = ArrayBuffer.empty[RDD[Row]]
    arr ++= rdds

    val stream = new SimpleDStream(ssc, arr)
    stream.foreachRDD(_.foreach(println))
    new Thread(new Runnable {
      override def run(): Unit = while (true) {
        (0 until 20).foreach { _ =>
          kafkaTestUtils.sendMessages("test", Array(s"0\t'a'\t$i"))
          i += 1
        }
        Thread.sleep(1000)
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = while (true) {
        arr.synchronized {
          arr ++= controller.search(15, "KAFKA_STU", "id,name,age")
        }
        Thread.sleep(5000)
      }
    }).start()
    ssc.start()
    ssc.awaitTermination()
    //    controller.start
    //    controller.awaitTermination

  }
}
