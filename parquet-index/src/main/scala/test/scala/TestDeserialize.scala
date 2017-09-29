package test.scala

import org.apache.spark.internal.Logging

class TestDeserialize extends Serializable with Logging {

  def transform[T, U](a: T)(f: T => U) = f(a)

  def readObject(): Unit = {
    logInfo("read Object~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  }
}
