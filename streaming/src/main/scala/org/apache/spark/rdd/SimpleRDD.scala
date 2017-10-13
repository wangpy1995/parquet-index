package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class SimpleRDD[T, U:ClassTag](
                       _sc: SparkContext,
                       rdd: RDD[U],
                       factor: T,
                       f: (T, U) => U) extends RDD[U](_sc, Nil) {
  override def compute(split: Partition, context: TaskContext) = {
    rdd.compute(split,context)
  }

  override protected def getPartitions =rdd.partitions
}

class SimplePartition(
                               idx: Int,
                               val partitions: Array[Partition]
                             ) extends Partition {
  override def index = idx
}