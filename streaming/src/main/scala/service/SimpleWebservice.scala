package service

import com.alibaba.fastjson.JSONArray
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.SimpleStreamingContext
import org.springframework.web.bind.annotation._
import service.SimpleWebservice._

import scala.collection.JavaConverters._

@RestController
@RequestMapping(value = Array("/stream"))
class SimpleWebservice {

  @ResponseBody
  @RequestBody(required = true)
  def createTable(@RequestParam sqlText: String): String = {
    val df = ssc.sql(sqlText)
    val fieldNames = df.schema.map(_.name)
    val jsons = new JSONArray()
    jsons.fluentAddAll(fieldNames.toList.asJavaCollection)
    jsons.toJSONString
  }

  @ResponseBody
  @RequestBody(required = true)
  def select(@RequestParam sqlText: String): Unit = {
    val df = ssc.sql(sqlText)
    val values = df.collectAsList()
    val jsons = new JSONArray()
    jsons.fluentAddAll(values)
    jsons.toJSONString
  }

}


object SimpleWebservice {
  val sparkConf = new SparkConf().setAppName("simple").setMaster("local[*]")
  val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val ssc = new SimpleStreamingContext(ss, Seconds(10))
}