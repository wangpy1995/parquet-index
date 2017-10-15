package org.apache.spark.sql.execution.datasources.test

import com.stream.task.{AtomRuleNode, MultiRule, Rule, SingleRule}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.SimpleUnionRDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.io.StdIn

class SimpleRDDTestSuite extends FunSuite {

  private lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("simple")
  private lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()


  test("simple") {
    val testData = 1 to 10 toArray
    val sc = ss.sparkContext
    val srcRdd = sc.parallelize(testData)
    val rdds = srcRdd.filter(_ % 3 == 0) :: srcRdd.filter(_ * 2 > 18) :: srcRdd.map(_ * 2) :: Nil
    new SimpleUnionRDD(sc, rdds).foreach(println)
    StdIn.readLine()
  }

  test("rule") {
    //int string float char double
    val data = (0 to 10).map(src => Map("int" -> src, "str" -> src.toString, "float" -> src.toFloat, "char" -> src.toChar, "double" -> src.toDouble))

    def createFunc[K, T](key: K) = (a: T, b: Map[K, Any]) => b.get(key) match {
      case Some(x) if x.asInstanceOf[Int] == a => b
    }

    val f1 = new AtomRuleNode("int", 1)
    val f2 = new AtomRuleNode("str", 2.toString)
    val f3 = new AtomRuleNode("char", 3.toChar)
    val f4 = new AtomRuleNode("float", 4.toFloat)
    val f5 = new AtomRuleNode("double", 5.toDouble)

    val r1 = SingleRule(-1, List(f1))
    val r2 = SingleRule(0, List(f1, f3, f4))
    val r3 = SingleRule(1, List(f3, f5))
    val r4 = SingleRule(0, List(f4, f5))
    val r5 = SingleRule(1, List(f1, f2, f5))

    val m1 = MultiRule(0, List(r1, r2, r3)).asInstanceOf[Rule]
    val m3 = MultiRule(1, List(m1, r3, r4, r5)).asInstanceOf[Rule]

    def unpack(rule: Rule): List[SingleRule] = rule match {
      case sr: SingleRule => List(sr)
      case MultiRule(_, rules: List[Rule]) => {
        rules match {
          case l@head :: tails => l.flatMap(unpack)
          case List(head) => unpack(head)
        }
      }.distinct
    }

    val rules = unpack(m3)
    rules
  }
}