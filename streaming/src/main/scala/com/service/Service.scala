package com.service

import org.apache.spark.sql.DataFrame

trait Simple {

  def submitTask(sqlText: String)

  def sendMessage(topic: String, id: Int, name: String, age: Int)

  def sql(sqlText: String): DataFrame
}

trait SimpleService {
  self: Simple =>

  def submit(sqlText: String) = self.submitTask(sqlText)

  def put(topic: String, id: Int, name: String, age: Int): Unit =self.sendMessage(topic, id, name, age)

  def get(): String

}
