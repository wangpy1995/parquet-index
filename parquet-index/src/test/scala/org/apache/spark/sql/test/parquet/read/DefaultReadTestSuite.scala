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

    def arr = (0 to 60000).mkString(",")

    /*    ss.sql(
          s"""
             |SELECT * FROM df WHERE key=1 AND value in ($arr)
          """.stripMargin).show()*/

    ss.sql(
      """
        |SELECT value FROM df
        |WHERE key=1 AND value in ('a','b')
      """.stripMargin).collect()
    StdIn.readLine()
  }

  test("insert") {
    val df = ss.read.parquet(path)
    df.createTempView("df")
    import ss.implicits._
    ss.sparkContext.parallelize(Array((1, "x"), (2, "y"))).toDF("key", "value").createOrReplaceTempView("d")

    ss.sql(
      """
        |INSERT INTO df PARTITION(key=1)
        |SELECT value FROM d
      """.stripMargin)

  }
}
