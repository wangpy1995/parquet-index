package service

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import com.stream.task.SingleTask
import org.apache.spark.rdd.SimpleRDD
import org.apache.spark.sql.Row
import org.springframework.beans.factory.annotation.{Autowired, Qualifier}
import org.springframework.stereotype.Service

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

@Path("/stream")
@Produces(Array(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML))
@Consumes(Array(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML))
@Service("restSimpleService")
class SimpleController {

  @Autowired
  @Qualifier("simpleWSService")
  @BeanProperty  var simpleService: SimpleService = _

  @POST
  @Path("/admin")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def executeSql(@FormParam("sql") sqlText: String): String = {
    val df = simpleService.sql(sqlText)
    /*val fieldNames = df.schema.map(_.name)
    val jsons = new JSONArray()
    jsons.fluentAddAll(fieldNames.toList.asJavaCollection)
    jsons.toJSONString*/
    df.schema.map(_.name).mkString("\t")
  }

  def sql(sqlText:String) = simpleService.sql(sqlText).rdd

  @POST
  @Path("/user")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def search(@FormParam("age") age: Int,
             @FormParam("tableList") tableList: String,
             @FormParam("requiredColumns") requiredColumns: String) = {
    val tables = tableList.split(",")
    val requiredParams = requiredColumns.split(",")
    val f = (a: Int, b: Row) => if (a == b.getAs[Int]("age")) b else null
    val task = new SingleTask[Int, Row](age, f)
    val results = ArrayBuffer.empty[Row]
    /*val inputStream = SimpleService.createDStreams(task, tables.toSeq, requiredParams, results)(f)
    inputStream.foreach(_.foreachRDD(_.foreach(println)))
    results.mkString("\n")*/
    simpleService.createRDD(task, tables.toSeq, requiredParams, f)
  }

  def start = simpleService.start

  def awaitTermination = simpleService.awaitTermination
}
