package test.other

import org.apache.spark.Partitioner
import org.scalatest.FunSuite
import test.parquet._

import scala.collection.{immutable, mutable}
import scala.io.StdIn

class RDDActionTestSuite extends FunSuite {
  test("action") {
    val testData = (0 to 1000).toArray
    /*flatMap { data =>
      val key = data % 2
      Map(key->data)
    }*/
    ss.sparkContext.parallelize(testData, 10).map(data => (data % 2, data)).mapPartitions { datas =>
      new MyIterator[Int, Int](datas).combineByKey()
    }.foreach(println)
    StdIn.readLine()
  }
}

class MyIterator[K, V](it: Iterator[(K, V)]) {
  assert(it.nonEmpty)

  def combineBy[A](f: ((K, V)) => A): Iterator[(A, List[V])] = {
    val map = mutable.Map.empty[A, mutable.Builder[V, List[V]]]

    def builder = List.newBuilder[V]

    for (i <- it) {
      val k = f(i)
      val bldr = map.getOrElseUpdate(k, builder)
      bldr += i._2
    }
    val b = immutable.Map.newBuilder[A, List[V]]
    for ((k, v) <- map) {
      b += ((k, v.result()))
    }
    b.result().iterator
  }

  def combineByKey() = combineBy(_._1)
}
