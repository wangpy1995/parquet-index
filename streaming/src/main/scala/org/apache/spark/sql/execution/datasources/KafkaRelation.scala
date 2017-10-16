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

  private lazy val preferBrokers = if (options.parameters.getOrElse("preferBrokers", true).asInstanceOf[Boolean])
    getBrokers
  else
    java.util.Collections.emptyMap[TopicPartition, String]()

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = userSpecifiedSchema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = insertFunc(data, Some(userSpecifiedSchema), options)

  override def buildScan() = {
    val availableOffsetRanges = getAvailableOffsetRanges()
    val broadcastSchema = sparkSession.sparkContext.broadcast(userSpecifiedSchema)
    val rdd = new KafkaRDD[Object, Object](
      sparkSession.sparkContext,
      options.asKafkaParams,
      availableOffsetRanges.toArray,
      preferBrokers,
      false
    ).mapPartitions { records =>
      records.map(record =>
        Row.fromSeq(KafkaRelation.convertString(record.value().asInstanceOf[String].split("\t"), broadcastSchema.value)))
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

  def getBrokers = {
    val c = consumer
    val result = new java.util.HashMap[TopicPartition, String]()
    val hosts = new java.util.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator()
    while (assignments.hasNext()) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator()
        while (infos.hasNext()) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    }
    result
  }

  def fetchTopicPartitions(): Set[TopicPartition] = {
    // Poll to get the latest assigned partitions
    if (currentOffsets.isEmpty) {
      try {
        consumer.poll(0)
      } catch {
        case e: NoOffsetForPartitionException => {
          e.getMessage
        }
      }
    }
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    partitions.asScala.toSet
  }

  def getAvailableOffsetRanges() = {
    val topicPartitions = fetchTopicPartitions()
    topicPartitions.map { tp =>
      assert(tp != null)
      consumer.seekToBeginning(Set(tp).asJava)
      val earliestOffset = currentOffsets.getOrElse(tp, consumer.position(tp))
      consumer.seekToEnd(Set(tp).asJava)
      val latestOffset = consumer.position(tp)
      OffsetRange(tp, earliestOffset, latestOffset)
    }
  }
}

object KafkaRelation extends Serializable {
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