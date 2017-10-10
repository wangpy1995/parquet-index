package org.apache.spark.sql.execution.datasources

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.{KafkaRDD, OffsetRange}

import scala.collection.JavaConverters._

case class KafkaRelation(options: KafkaOption, userSpecifiedSchema: StructType, insertFunc: (DataFrame, Option[StructType], KafkaOption) => Unit)(@transient val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation with TableScan {

  private val consumer = createConsumer()
  private var currentOffsets: Map[TopicPartition, Long] = Map[TopicPartition, Long]()

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = userSpecifiedSchema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = insertFunc(data, Some(userSpecifiedSchema), options)

  override def buildScan() = {
    val availableOffsetRanges = getAvailableOffsetRanges()
    val rdd = new KafkaRDD[Object, String](
      sparkSession.sparkContext,
      options.asKafkaParams,
      availableOffsetRanges.toArray,
      java.util.Collections.emptyMap[TopicPartition, String](),
      true
    ).mapPartitions { records =>
      records.map(record => Row.fromSeq(record.value().split("\t")))
    }
    currentOffsets = currentOffsets ++ availableOffsetRanges.map(range => range.topicPartition() -> range.untilOffset)
    rdd
  }

  private def createConsumer(): Consumer[Object, String] = synchronized {
    val newKafkaParams = new java.util.HashMap[String, Object](options.asKafkaParams)
    newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, options.groupId)
    val consumer = new KafkaConsumer[Object, String](newKafkaParams)
    consumer.subscribe(Seq(options.topic).asJava)
    consumer
  }

  def fetchTopicPartitions(): Set[TopicPartition] = {
    // Poll to get the latest assigned partitions
    try {
      consumer.poll(0)
    } catch {
      case e: NoOffsetForPartitionException => {
        e.getMessage
      }
    }
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    partitions.asScala.toSet
  }

  def getAvailableOffsetRanges() = {
    val topicPartitions = fetchTopicPartitions()
    topicPartitions.map { tp =>
      consumer.seekToBeginning(Set(tp).asJava)
      val earliestOffset = currentOffsets.getOrElse(tp, consumer.position(tp))
      consumer.seekToEnd(Set(tp).asJava)
      val latestOffset = consumer.position(tp)
      OffsetRange(tp, earliestOffset, latestOffset)
    }
  }
}