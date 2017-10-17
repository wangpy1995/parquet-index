package org.apache.spark.sql.execution.datasources.test

import java.io.{File, FileWriter}

import com.service.SimpleWebService
import com.service.impl.SimpleService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.SimpleDStream
import org.apache.spark.utils.test.KafkaTestUtils
import org.scalatest.FunSuite
import server.TaskServer

import scala.io.StdIn
import scala.util.Random

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
  /*  private val kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

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
    }*/

  def main(args: Array[String]): Unit = {
    import SimpleService._
    val controller = new SimpleWebService
    @volatile var i = 0

    val fileStream = new SimpleDStream(ssc, arr)
    val file = new File("/home/wpy/tmp/stream/kafka_stream")
    if (!file.exists()) file.createNewFile()

    fileStream.foreachRDD(_.filter(_ != null).foreachPartition { rows =>
      val writer = new FileWriter(file, true)
      try
        rows.foreach { row =>
          writer.append(row.toString + "\n")
        }
      finally {
        writer.flush()
        writer.close()
      }
    })

    fileStream.foreachRDD(_.filter(_ != null).foreach { row =>
      results.synchronized {
        results += row.toString()
      }
    })
    ssc.start()
    new Thread(new Runnable {
      def run() = TaskServer.startServer()
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = while (true) {
        (0 until 20).foreach { _ =>
          controller.putMessage("test", i, (i to i + 7).map(x => (x % (Random.nextInt(26)+1)+97).toChar).mkString(""), Random.nextInt(i + 1) % 31)
          i += 1
        }
        Thread.sleep(1000)
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = while (true) {
        controller.submitSqlTask(s"select * from test where age = 25")
        Thread.sleep(1000)
      }
    }).start()
    ssc.awaitTermination()
  }
}
