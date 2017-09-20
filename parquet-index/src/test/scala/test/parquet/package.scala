package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object parquet {
  private[parquet] lazy val testData = Array((1, "a"), (2, "b"), (3, "c"))
  private[test] lazy val sparkConf = new SparkConf().setAppName("parquet-index").setMaster("local[*]")
  private[test] lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private[parquet] val path = "/home/wpy/tmp/test_parquet"
}
