package com.service

import org.apache.spark.sql.DataFrame

trait Simple {

  protected def submitTask(sqlText: String): Unit

  protected def sendMessage(topic: String, id: Int, name: String, age: Int,cacheProducer:Boolean): Unit

  protected def sql(sqlText: String): DataFrame
}

trait SimpleComponent {
  self: Simple =>

  protected def submit(sqlText: String) = self.submitTask(sqlText)

  protected def put(topic: String, id: Int, name: String, age: Int): Unit = self.sendMessage(topic, id, name, age,cacheProducer = true)

}

trait Service {
  protected def get(): Seq[String]
}

trait ServiceComponent {
  self: Service =>
  protected def getResult():Seq[String] = self.get()
}
