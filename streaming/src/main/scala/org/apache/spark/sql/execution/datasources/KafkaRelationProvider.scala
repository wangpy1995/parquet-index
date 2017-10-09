package org.apache.spark.sql.execution.datasources

import java.util.{Locale, Properties}

import kafka.admin.{AdminClient, AdminUtils, TopicCommand}
import kafka.common.Topic
import kafka.utils.ZkUtils
import kafka.utils.ZkUtils.getDeleteTopicPath
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.{CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

class KafkaRelationProvider[K, V](zkClient: ZkClient) extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  private val zkUtils = ZkUtils(zkClient, JaasUtils.isZkSecurityEnabled)

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame) = {
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
    val options = new KafkaOption(parameters)
    val topic = options.asProperties.getProperty(KafkaOption.TOPIC_NAME)
    assert(topic.nonEmpty, "Topic is empty")
    //throws exception if group not exists
    val exists = zkUtils.getAllTopics().contains(topic)

    if (exists)
      mode match {
        case SaveMode.Overwrite =>
          assert(!Topic.isInternal(topic), s"Topic $topic is a kafka internal topic and is not allowed to be marked for deletion.")
          val (numPartitions, replicationFactor) = zkUtils.getPartitionAssignmentForTopics(Seq(topic)).get(topic) match {
            case Some(topicPartitionAssignment) =>
              (topicPartitionAssignment.size, topicPartitionAssignment.head._2.size)
          }
          //delete
          zkUtils.createPersistentPath(getDeleteTopicPath(topic))
          //create
          AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor)
        case SaveMode.Append =>

        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"Table or view (topic_name) '${options.topic}' already exists. SaveMode: ErrorIfExists.")

        case SaveMode.Ignore =>
      }

  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = ???

  override def shortName() = "kafka"

  def save(df: DataFrame,
           tableSchema: Option[StructType],
           f: Row => V,
           isCaseSensitive: Boolean,
           options: KafkaOption
          ) = {
    val rddSchema = df.schema
    df.foreachPartition { iter =>
      val producer = new KafkaProducer[K, V](options.asProperties)
      iter.map { row =>
        producer.send(new ProducerRecord[K, V](options.topic, f(row)))
      }
    }
  }
}

class KafkaOption(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  def topic = asProperties.getProperty(KafkaOption.TOPIC_NAME)
}

object KafkaOption {
  private val kafkaOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kafkaOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val TOPIC_NAME = newOption("topic")
  val BOOTSTRAP_SERVERS = newOption("bootStrap.servers")
  val KEY_DESERIALIZER = newOption("key.deserializer")
  val VALUE_DESERIALIZER = newOption("value.deserializer")
  val GROUP_ID = newOption("group.id")
}