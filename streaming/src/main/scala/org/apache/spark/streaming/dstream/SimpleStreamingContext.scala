package org.apache.spark.streaming.dstream

import java.util

import com.stream.task.{Single, Task}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.annotation.meta.param
import scala.reflect.ClassTag

class SimpleStreamingContext(@(transient@param) ss: SparkSession,
                             @(transient@param) duration: Duration
                            ) extends StreamingContext(ss.sparkContext, duration) {
  type T

  def sql(sqlText: String) = ss.sql(sqlText)

  def simpleTaskDStream[U:ClassTag](task: Task, tables: String, requiredFactors: Seq[String])(f: (T, U) => U) = {
    val rdd = simpleRDD(task, tables, requiredFactors, f)
    val arr = new util.ArrayDeque[RDD[U]]()
    arr.add(rdd.asInstanceOf[RDD[U]])
    new SimpleDStream(this, arr)
  }

  def simpleRDD[U:ClassTag](task: Task, table: String, requiredFactors: Seq[String], f: (T, U) => U) = {
    val rdd = sql(s"select ${requiredFactors.mkString(",")} from $table").rdd.asInstanceOf[RDD[U]]
    task match {
      case Single(term: T, _) =>
        rdd.map(f(term, _))
    }
  }

}
