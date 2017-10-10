package org.apache.spark.sql.execution.datasources

import java.util
import java.util.{Locale, Properties}

import kafka.admin.AdminUtils
import kafka.common.Topic
import kafka.utils.ZkUtils
import kafka.utils.ZkUtils.getDeleteTopicPath
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.KafkaOption._
import org.apache.spark.sql.sources.{CreatableRelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class KafkaDatasource
  extends CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {

  private var tableSchema: StructType = _

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame) = {
    val options = new KafkaOption(parameters)
    val zkClient = options.zkClient
    val zkUtils = ZkUtils(zkClient, JaasUtils.isZkSecurityEnabled)

    val topic = options.topic
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
          //TODO table schema is needed
          if (tableSchema == null) tableSchema = data.schema
          save(data, Some(tableSchema), options)
        case SaveMode.Append =>
          if (tableSchema == null) tableSchema = data.schema
          save(data, Some(tableSchema), options)
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"Table or view (topic_name) '${options.topic}' already exists. SaveMode: ErrorIfExists.")

        case SaveMode.Ignore =>
      } else {
      AdminUtils.createTopic(zkUtils, topic, 3, 3)
      createRelation(sqlContext, mode, parameters, data)
    }
    createRelation(sqlContext, parameters, data.schema)
  }

  override def shortName() = "kafka"

  def save(df: DataFrame,
           tableSchema: Option[StructType],
           options: KafkaOption
          ) = {
    df.foreachPartition { iter =>
      val producer = new KafkaProducer[Object, Object](options.asProperties)
      iter.foreach { row =>
        producer.send(new ProducerRecord(options.topic, row.toSeq.mkString("\t")))
      }
      producer.flush()
      producer.close()
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    tableSchema = schema
    val options = new KafkaOption(parameters)
    KafkaRelation(options, schema, save)(sqlContext.sparkSession)
  }
}

class KafkaOption private(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  import collection.JavaConverters._

  def asKafkaParams: util.Map[String, Object] = {
    val kafkaParams = parameters.originalMap ++ Map("auto.offset.reset" -> "none")
    new java.util.HashMap[String, Object](kafkaParams.asJava)
  }

  def topic = asProperties.getProperty(TOPIC_NAME)

  def groupId = asProperties.getProperty(GROUP_ID)

  def zkClient = new ZkClient(asProperties.getProperty(ZOOKEEPER_ADDRESS))
}

object KafkaOption {
  private val kafkaOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kafkaOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val TOPIC_NAME = newOption("topic")
  val BOOTSTRAP_SERVERS = newOption("bootstrap.servers")
  val KEY_DESERIALIZER = newOption("key.deserializer")
  val VALUE_DESERIALIZER = newOption("value.deserializer")
  val GROUP_ID = newOption("group.id")
  val ZOOKEEPER_ADDRESS = newOption("zookeeper.address")
}