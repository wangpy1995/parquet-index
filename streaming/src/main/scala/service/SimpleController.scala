package service

import com.stream.task.SingleTask
import org.apache.spark.sql.Row
import org.springframework.web.bind.annotation._

import scala.collection.mutable.ArrayBuffer

@RestController
@RequestMapping(value = Array("/stream"))
class SimpleController {

  @RequestMapping(value = Array("/admin"))
  @RequestBody(required = true)
  @ResponseBody
  def executeSql(@RequestParam sqlText: String): String = {
    val df = SimpleService.sql(sqlText)
    /*val fieldNames = df.schema.map(_.name)
    val jsons = new JSONArray()
    jsons.fluentAddAll(fieldNames.toList.asJavaCollection)
    jsons.toJSONString*/
    df.schema.map(_.name).mkString("\t")
  }

  @RequestMapping(value = Array("/user"))
  @RequestBody(required = true)
  @ResponseBody
  def search(@RequestParam age: Int, @RequestParam tableList: String, @RequestParam requiredParamList: String) = {
    val tables = tableList.split(",")
    val requiredParams = requiredParamList.split(",")
    val f = (a: Int, b: Row) => if (a == b.getAs[Int]("age")) b else b
    val task = new SingleTask[Int, Row](age, f)
    val results = ArrayBuffer.empty[Row]
    /*val inputStream = SimpleService.createDStreams(task, tables.toSeq, requiredParams, results)(f)
    inputStream.foreach(_.foreachRDD(_.foreach(println)))
    results.mkString("\n")*/
    SimpleService.createRDD(task,tables.toSeq,requiredParams,f)
  }

  def start = SimpleService.start

  def awaitTermination = SimpleService.awaitTermination
}
