package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class SimpleRDD[T, U: ClassTag](
                                 _sc: SparkContext,
                                 rdd: RDD[U],
                                 factor: T,
                                 f: (T, U) => U) extends RDD[U](_sc, Nil) {
  assert(rdd != null && factor != null)

  override def compute(split: Partition, context: TaskContext) = {
    rdd.compute(split, context).map { row =>
      f(factor, row)
    }
  }

  override protected def getPartitions = rdd.partitions
}

class SimplePartition(
                       idx: Int,
                       val partitions: Array[Partition]
                     ) extends Partition {
  override def index = idx
}

class SimpleUnionRDD[T](
                         _sc: SparkContext,
                         rdds: Seq[RDD[T]],
                       ) extends RDD[T](_sc, Nil) {
  assert(rdds.nonEmpty)

  override def compute(split: Partition, context: TaskContext) = {
    val simpleUnionPartition = split.asInstanceOf[SimpleUnionPartition]
    simpleUnionPartition.simplePartitions.zipWithIndex.flatMap{
      case (simplePartition,i)=>simplePartition.partitions.flatMap(rdds(i).compute(_,context))
    }.iterator
  }

  override protected def getPartitions = Array(new SimpleUnionPartition(rdds.zipWithIndex.map {
    case (rdd, index) => new SimplePartition(index, rdd.partitions)
  }.toArray))
}

class SimpleUnionPartition(val simplePartitions: Array[SimplePartition]) extends Partition {
  override def index = 0
}