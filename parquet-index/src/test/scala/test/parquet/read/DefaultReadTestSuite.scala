package test.parquet.read

import org.scalatest.FunSuite
import test.parquet._
import ss.implicits._
class DefaultReadTestSuite extends FunSuite {
  test("default_read") {
    val df = ss.read.parquet(path)
    df.createTempView("df")
    val d = ss.createDataset(Array((4,"d"),(5,"e"))).toDF("key","value")
    df.union(d).distinct().createTempView("d")
    /*ss.sql(
    """
        |INSERT OVERWRITE TABLE df
        |SELECT * FROM d
      """.stripMargin)*/
    ss.sql(
      """
        |SELECT * FROM d
      """.stripMargin).show()
  }
}
