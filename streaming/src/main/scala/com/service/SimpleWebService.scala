package com.service

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import com.alibaba.fastjson.{JSON, JSONArray}
import com.service.impl.SimpleService
import com.service.response.BaseResponse
import org.springframework.web.bind.annotation.RequestParam

//@Service("restSimpleService")
class SimpleWebService {

  //  @Autowired
  //  @Qualifier("simpleService")
  val simpleService: SimpleService = new SimpleService

  @POST
  @Path("/admin")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.TEXT_PLAIN))
  def executeSql(@RequestParam sqlText: String): String = {
    val df = simpleService.sql(sqlText)
    val jsons = new JSONArray()
    val response = new BaseResponse()
    response.setMsg(df.schema.map(_.name).mkString("\t"))
    response.setResultCode(0)
    jsons.add(response)
    jsons.toJSONString
  }

  @GET
  @Path("/sql/{sqlText}")
  def task(@PathParam("sqlText") sqlText: String): String = tryJSONResponse {
    simpleService.submitTask(sqlText)
    "submit task succeed"
  }

  @GET
  @Path("/msg/{topic}/{id}/{name}/{age}")
  def insert(@PathParam("topic") topic: String,
             @PathParam("id") id: Int,
             @PathParam("name") name: String,
             @PathParam("age") age: Int) = tryJSONResponse {
    simpleService.insert(topic, id, name, age)
    "insert succeed"
  }

  @GET
  @Path("/results")
  def results() = tryJSONResponse {
    simpleService.getResults
  }

  private def tryJSONResponse[T](f: => String): String = {
    val response = new BaseResponse()
    try {
      val msg = f
      response.setMsg(msg)
      response.setResultCode(ErrorCode.success)
    } catch {
      case e: Throwable =>
        response.setMsg(e.getCause.getMessage)
        response.setResultCode(ErrorCode.error)
    }
    JSON.toJSONString(response, true)
  }

  /*  def search(@RequestParam("age") age: Int,
               @RequestParam("tableList") tableList: String,
               @RequestParam("requiredColumns") requiredColumns: String) = {
      val tables = tableList.split(",")
      val requiredParams = requiredColumns.split(",")
      val f = (a: Int, b: Row) => if (a == b.getAs[Int]("age")) b else null
      val task = new Single[Int, Row](age, f)
      val results = ArrayBuffer.empty[Row]
      /*val inputStream = SimpleService.createDStreams(task, tables.toSeq, requiredParams, results)(f)
      inputStream.foreach(_.foreachRDD(_.foreach(println)))
      results.mkString("\n")*/
      simpleService.createRDD(task, tables.toSeq, requiredParams, f)
    }*/

  @GET
  @Path("/print")
  def get(): Unit = {
    println("xxxxxxxxxxxxxxxx")
  }
}
