package test.parquet.write

import org.scalatest.FunSuite
import test.parquet._

class DefaultWriteTestSuite extends FunSuite{

  test("default_write"){
    import ss.implicits._
    val df = ss.sparkContext.parallelize(testData).coalesce(1).toDF("key","value")
    df.write.parquet(path)
  }
}
