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
    ss.sql(
      s"""
        |SELECT * FROM df WHERE key=1 AND value in ($arr)
      """.stripMargin).show()

    ss.sql(
      """
        |SELECT * FROM df WHERE key=1 AND value in ('a','b')
      """.stripMargin).show()
    StdIn.readLine()
  }
}
