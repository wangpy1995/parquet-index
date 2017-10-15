package org.apache.spark.streaming.dstream

import java.util.concurrent.CountDownLatch

import org.apache.spark.rdd.{RDD, SimpleUnionRDD}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SimpleDStream[T: ClassTag](_ssc: StreamingContext, buffer: ArrayBuffer[RDD[T]]) extends InputDStream[T](_ssc) {

  override def slideDuration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  override def dependencies = List()

  override def compute(validTime: Time) = {
    val tmp = new ArrayBuffer[RDD[T]]()
    tmp ++= buffer
    buffer.clear()
    if (tmp.nonEmpty) {
      Some(new SimpleUnionRDD(_ssc.sparkContext, tmp))
    } else {
      Some(_ssc.sparkContext.emptyRDD)
    }
  }


  override def generateJob(time: Time): Option[Job] = {
    getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          val futures = rdd.asInstanceOf[SimpleUnionRDD[T]].rdds.map { rdd =>
            _ssc.sparkContext.submitJob(rdd,
              (_: Iterator[T]) => {},
              0 to rdd.getNumPartitions,
              (_: Int, _: Unit) => {}, {})
          }
          val countDownLatch = new CountDownLatch(futures.length - 1)
          futures.foreach(_.onComplete { _ =>
            countDownLatch.countDown()
          }(ThreadUtils.sameThread))
          countDownLatch.await()
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}
}

