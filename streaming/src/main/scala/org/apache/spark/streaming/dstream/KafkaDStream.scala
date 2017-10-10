package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.{StreamingContext, Time}

class KafkaDStream[T](_ssc: StreamingContext, rdd: RDD[T], func: Iterator[T] => Unit) extends InputDStream[T](_ssc) {
  override def slideDuration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  override def dependencies = List()

  override def compute(validTime: Time) = Some(rdd)

  override def generateJob(time: Time): Option[Job] = {
    val jobFunc = () => _ssc.sparkContext.submitJob(rdd,
      func,
      0 to rdd.getNumPartitions,
      (_, _) => {}, {})
    Some(new Job(time, jobFunc))
  }
}
