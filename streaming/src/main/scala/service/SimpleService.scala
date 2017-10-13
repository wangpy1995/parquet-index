package service

import com.stream.task.Task
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.SimpleStreamingContext

import scala.collection.mutable.ArrayBuffer

object SimpleService {

  private val sparkConf = new SparkConf().setAppName("simple").setMaster("local[*]")
  private val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val ssc = new SimpleStreamingContext(ss, Seconds(10))


  def createDStreams[T](task: Task, tables: Seq[String], requiredFactors: Seq[String], results: ArrayBuffer[Row])(f: (T, Row) => Row) = {
    tables.map(ssc.simpleTaskDStream(task, _, requiredFactors)(f))
  }

  def createRDD[T](task: Task, tables: Seq[String], requiredFactors: Seq[String], f: (T, Row) => Row) =
    tables.map(ssc.simpleRDD(task, _, requiredFactors, f))


  def sql(sqlText: String) = ssc.sql(sqlText)


  def start = ssc.start()

  def awaitTermination = ssc.awaitTermination()

}
