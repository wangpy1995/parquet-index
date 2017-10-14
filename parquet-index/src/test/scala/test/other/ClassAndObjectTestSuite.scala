package test.other

;

import org.scalatest.FunSuite
import test.scala.TestInnerObject;

class ClassAndObjectTestSuite extends FunSuite {

  test("innerObj") {
    val a = new TestInnerObject("aaaaaaa")
    a.innerObject("11111").printIdentifiers
    val b = new TestInnerObject("bbbbbb")
    b.innerObject("22222").printIdentifiers
    val c = new TestInnerObject("cccccc")
    c.innerObject("33333").printIdentifiers

    c.innerObject match {
      /*   case a.InnerObject(x) =>
           println("a " + x.mkString(","))
         case b.InnerObject(x) =>
           println("b " + x.mkString(","))*/
      case c.innerObject(x) =>
        println("c " + x.mkString(","))
      case _ =>
    }

    assert(a.innerObject != b.innerObject)
    assert(a.innerObject != c.innerObject)
    assert(b.innerObject != c.innerObject)
  }

  test("list") {
    val a = List(1, 2, 3, 4, 5, 6)
    val b = List(1)
    b match {
      case seq@head :: body :: tail =>
        seq.foreach(println)
      case head::Nil =>
        println(head)
      case _ =>
    }
  }
}
