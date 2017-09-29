package test.other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import test.scala.TestDeserialize

import scala.collection.mutable
import scala.io.StdIn

class RDDPersistTestSuite extends FunSuite {
  private lazy val sparkConf = new SparkConf().setMaster("local[*]")
    .setAppName("persist")

  private lazy val sc = SparkContext.getOrCreate(sparkConf)
  private val CACHE = new mutable.HashMap[String, RDD[Int]]()
  private val SET = new mutable.HashSet[RDD[Int]]()

  test("persist") {
    sc.setCheckpointDir("file:/home/wangpengyu6/tmp/checkpoint/")

    (0 until 144).foreach(i =>
      cacheRDD("kafka", sc.parallelize(i * 10000 until (i + 1) * 10000).coalesce(i % 4 + 1))
    )
    StdIn.readLine()
  }

  test("deserialize") {
    val ser = new TestDeserialize()
    val res = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 8, 7, 9)).mapPartitions { iter =>
      iter.map(i => ser.transform(i)(j => j - 1))
    }.collect()
    println(res.mkString(","))
  }

  def cacheRDD(key: String, rdd: RDD[Int]): Unit = {
    CACHE.get(key) match {
      case Some(srcRDD: RDD[Int]) => {
        rdd.checkpoint()
        //        SET += rdd
        //        cacheRDD = SET.reduce(_ union _)
        val cacheRDD = (srcRDD union rdd) coalesce srcRDD.getNumPartitions cache()

        println("cacheRDD: " + cacheRDD.count())
        srcRDD.unpersist()
        CACHE.put(key, cacheRDD)
      }

      case None => {
        rdd.cache()
        println("rdd: " + rdd.count())
        CACHE.put(key, rdd)
      }
    }
  }
}
