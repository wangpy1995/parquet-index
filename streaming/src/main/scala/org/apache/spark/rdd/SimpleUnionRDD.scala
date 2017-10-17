package org.apache.spark.rdd

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SimpleUnionRDD[T: ClassTag](
                                   _sc: SparkContext,
                                   val rdds: Seq[RDD[T]]
                                 ) extends RDD[T](_sc, Nil) {
  assert(rdds.nonEmpty)

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val simpleUnionPartition = split.asInstanceOf[SimpleUnionPartition]
    simpleUnionPartition
      .partitions
      .flatMap(p => rdds(simpleUnionPartition.rddIndex).preferredLocations(p))
  }

  override def compute(split: Partition, context: TaskContext) = {
    val simpleUnionPartition = split.asInstanceOf[SimpleUnionPartition]
    simpleUnionPartition
      .partitions
      .flatMap(rdds(simpleUnionPartition.rddIndex).compute(_, context))
      .iterator
  }

  override protected def getPartitions = {
    var rddIdx = 0
    var index = 0
    rdds.map { rdd =>
      val partition = new SimpleUnionPartition(index, rddIdx, rdd.partitions)
      rddIdx += 1
      index += 1
      partition
    }.toArray
  }
}

class SimpleUnionPartition(idx: Int,
                           val rddIndex: Int,
                           val partitions: Array[Partition]) extends Partition {
  override def index = idx
}