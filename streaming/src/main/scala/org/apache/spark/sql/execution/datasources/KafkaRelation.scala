package org.apache.spark.sql.execution.datasources

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.{KafkaRDD, OffsetRange}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

case class KafkaRelation(options: KafkaOption,
                         userSpecifiedSchema: StructType,
                         insertFunc: (DataFrame, Option[StructType], KafkaOption) => Unit
                        )(@transient val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation with TableScan {

  private val consumer = createConsumer()
  private var currentOffsets: Map[TopicPartition, Long] = Map[TopicPartition, Long]()

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = userSpecifiedSchema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = insertFunc(data, Some(userSpecifiedSchema), options)

  override def buildScan() = {
    val availableOffsetRanges = getAvailableOffsetRanges()
    val tpes = sparkSession.sparkContext.broadcast(userSpecifiedSchema)
    val rdd = new KafkaRDD[Object, Object](
      sparkSession.sparkContext,
      options.asKafkaParams,
      availableOffsetRanges.toArray,
      java.util.Collections.emptyMap[TopicPartition, String](),
      true
    ).mapPartitions { records =>
      records.map(record =>
        Row.fromSeq(KafkaRelation.convertString(record.value().asInstanceOf[String].split("\t"),tpes.value)))
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

object KafkaRelation extends Serializable{
  def convertString(src: Seq[String], schema: StructType): Seq[Any] = {
    schema.zipWithIndex.map { sf =>
      val i = sf._2
      sf._1.dataType match {
        case IntegerType => src(i).toInt
        case LongType => src(i).toLong
        case ByteType => src(i).toByte
        case DoubleType => src(i).toDouble
        case FloatType => src(i).toFloat
        case BooleanType => src(i).toBoolean
        case ShortType => src(i).toShort
        case _ => UTF8String.fromString(src(i))
      }
    }
  }
}