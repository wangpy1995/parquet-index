package org.apache.spark.streaming.dstream

import com.stream.task.{SingleTask, Task}
import org.apache.spark.rdd.{RDD, SimpleRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.annotation.meta.param
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SimpleStreamingContext(@(transient @param) ss: SparkSession,
                             @(transient @param) duration: Duration
                            ) extends StreamingContext(ss.sparkContext, duration) {
  def sql(sqlText: String) = ss.sql(sqlText)

  def simpleTaskDStream[T, U:ClassTag](task: Task, tables: String, requiredFactors: Seq[String])(f: (T, U) => U) = {
    val rdd = simpleRDD(task, tables, requiredFactors, f)
    val arr = new ArrayBuffer[RDD[U]]()
    arr += rdd.asInstanceOf[RDD[U]]
    new SimpleDStream(this, arr)
  }

  def simpleRDD[T, U:ClassTag](task: Task, table: String, requiredFactors: Seq[String], f: (T, U) => U) = {
    val rdd = sql(s"select ${requiredFactors.mkString(",")} from $table").rdd.asInstanceOf[RDD[U]]
    task match {
      case SingleTask(term: T, _) =>
        new SimpleRDD(sparkContext, rdd, term, f)
    }
  }

}
