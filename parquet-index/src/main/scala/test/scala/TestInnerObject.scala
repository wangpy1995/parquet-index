package test.scala

import scala.collection.mutable

class TestInnerObject(x: String) {
  println(s"Class say something: $x")

  object innerObject {
    val identifiers = new mutable.HashSet[String]()
    identifiers += x

    def apply(id: String) = {
      identifiers += id
      this
    }

    def unapply(x:this.type) = Some(x.identifiers)

    def printIdentifiers = println(identifiers.mkString(","))
  }

}
