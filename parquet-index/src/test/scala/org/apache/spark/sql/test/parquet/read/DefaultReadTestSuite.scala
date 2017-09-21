package org.apache.spark.sql.test.parquet.read

import org.apache.spark.sql.test.parquet._
import org.scalatest.FunSuite

import scala.io.StdIn
class DefaultReadTestSuite extends FunSuite {
  test("default_read") {
    val df = ss.read.parquet(path)
    df.createTempView("df")
    /*val d = ss.createDataset(Array((4,"d"),(5,"e"))).toDF("key","value")
    df.union(d).distinct().createTempView("d")*/
    /*ss.sql(
    """
        |INSERT OVERWRITE TABLE df
        |SELECT * FROM d
      """.stripMargin)*/
    ss.sql(
      """
        |SELECT * FROM df WHERE key in (1,2)
      """.stripMargin).rdd.count()
    StdIn.readLine()
  }
}
