package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{StreamingContext, Time}

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
      Some(new UnionRDD(_ssc.sparkContext, tmp))
    } else {
      Some(_ssc.sparkContext.emptyRDD)
    }
  }

  /* override def generateJob(time: Time): Option[Job] = {
     val emptyFunc = (iterator: Iterator[T]) => {}
     val jobFunc = () => _ssc.sparkContext.submitJob(rdd,
       emptyFunc,
       0 to rdd.getNumPartitions,
       (_: Int, _: Unit) => {}, {})
     Some(new Job(time, jobFunc))
   }*/

  override def start(): Unit = {}

  override def stop(): Unit = {}
}

