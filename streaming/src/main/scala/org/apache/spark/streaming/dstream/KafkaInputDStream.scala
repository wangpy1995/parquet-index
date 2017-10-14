package org.apache.spark.streaming.dstream

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.datasources.KafkaOption
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.scheduler.{Job, RateController}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.{StreamingContext, Time}

import collection.JavaConverters._

class KafkaInputDStream[K, V](
                               _ssc: StreamingContext,
                               options: KafkaOption,
                               maxRate: Long
                             ) extends DStream[ConsumerRecord[K, V]](_ssc) {
  private[streaming] var lastValidTime: Time = null
  private var currentOffsets: Map[TopicPartition, Long] = Map[TopicPartition, Long]()

  override def slideDuration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  override private[streaming] def isTimeValid(time: Time): Boolean = {
    if (!super.isTimeValid(time)) {
      false // Time not valid
    } else {
      // Time is valid, but check it is more than lastValidTime
      if (lastValidTime != null && time < lastValidTime) {
        logWarning(s"isTimeValid called with $time whereas the last valid time " +
          s"is $lastValidTime")
      }
      lastValidTime = time
      true
    }
  }

  override def dependencies = List()

  val id = ssc.getNewInputStreamId()

  protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  protected[streaming] def maxMessagesPerPartition(
                                                    offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate())

    // calculate a per-partition rate limit based on current lag
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
          val maxRateLimitPerPartition = maxRate
          val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)
          } else backpressureRate)
        }
      case None => offsets.map { case (tp, offset) => tp -> maxRate }
    }

    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) => tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }

  private val consumer = new KafkaConsumer[K, V](options.asKafkaParams)
  consumer.subscribe(Seq(options.topic).asJava)

  def latestOffsets(): Map[TopicPartition, Long] = {
    try {
      val partitions = consumer.assignment()
      currentOffsets = currentOffsets ++ partitions.asScala.diff(currentOffsets.keySet).map(tp => tp -> consumer.position(tp))
      consumer.pause(partitions)
      consumer.seekToEnd(currentOffsets.keySet.asJava)
      partitions.asScala.map(tp => tp -> consumer.position(tp)).toMap
    }
  }

  protected def clamp(
                       offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    maxMessagesPerPartition(offsets).map { mmp =>
      mmp.map { case (tp, messages) =>
        val uo = offsets(tp)
        tp -> Math.min(currentOffsets(tp) + messages, uo)
      }
    }.getOrElse(offsets)
  }

  protected def getBrokers = {
    val c = consumer
    val result = new java.util.HashMap[TopicPartition, String]()
    val hosts = new java.util.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator()
    while (assignments.hasNext) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator()
        while (infos.hasNext) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    }
    result
  }


  override def compute(validTime: Time) = {
    val untilOffsets = clamp(latestOffsets())
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = currentOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }
    val rdd = new KafkaRDD[K, V](
      context.sparkContext,
      options.asKafkaParams,
      offsetRanges.toArray,
      getBrokers
      , true)
    currentOffsets = untilOffsets
    val m = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    currentOffsets.foreach(tp => m.put(tp._1, new OffsetAndMetadata(tp._2)))
    if (!m.isEmpty) consumer.commitAsync(m, new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        if (exception != null) log.error("Offset commit failed.", exception)
      }
    })
    Some(rdd)
  }

/*  override def generateJob(time: Time): Option[Job] = {
    getOrCompute(time) match {
      case Some(rdd) =>
        val func = (a: Iterator[ConsumerRecord[K, V]]) => {}
        val jobFunc = () => _ssc.sparkContext.submitJob(rdd, func, 0 to rdd.getNumPartitions, (_, _) => {}, {})
        Some(new Job(time, jobFunc))
    }
  }*/

  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }

}
