package org.apache.spark.sql.execution.datasources.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

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
         |id string,
         |name string,
         |age string
         |)
         |USING org.apache.spark.sql.execution.datasources.KafkaRelationProvider
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

object Test{
  private lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
  private lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private val kafkaTestUtils = new KafkaTestUtils
  kafkaTestUtils.setup()
  //  kafkaTestUtils.sendMessages("test",Array("0\t'a'\t1"))
  def createKafkaTempTable(table: String) = {
    ss.sql(
      s"""
         |CREATE TEMPORARY VIEW $table(
         |id string,
         |name string,
         |age string
         |)
         |USING org.apache.spark.sql.execution.datasources.KafkaRelationProvider
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
      createKafkaTempTable("KAFKA_STU")
      ss.sql("select * from KAFKA_STU").show()
      while(true){
        val sql = StdIn.readLine()
        ss.sql(sql).show()
      }
  }
}
