package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.reflect.ClassTag

class KafkaDStream[T:ClassTag](_ssc: StreamingContext, rdd: RDD[T], func: Iterator[T] => Unit) extends InputDStream[T](_ssc) {
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
      (_:Int, _:Unit) => {}, {})
    Some(new Job(time, jobFunc))
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}
}
