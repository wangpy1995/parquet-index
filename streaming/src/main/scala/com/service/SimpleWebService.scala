package com.service

import java.util
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import com.alibaba.fastjson.{JSON, JSONObject}
import com.service.impl.SimpleImpl
import com.service.response.BaseResponse
import org.springframework.web.bind.annotation.RequestParam

//@Service("restSimpleService")
class SimpleWebService extends SimpleImpl with SimpleService {
  //  @Autowired
  //  @Qualifier("simpleService")
  //  val simpleService: SimpleWebServiceImpl = new SimpleWebServiceImpl

  @POST
  @Path("/admin")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.TEXT_PLAIN))
  def executeSql(@RequestParam sqlText: String): String = tryJSONResponse {
    val df = sql(sqlText)
    ("succeed", df.schema.map(_.name))
  }

  @GET
  @Path("/sql/{sqlText}")
  def submitSqlTask(@PathParam("sqlText") sqlText: String): String = tryJSONResponse {
    submit(sqlText)
    ("submit task succeed", null)
  }

  @GET
  @Path("/msg/{topic}/{id}/{name}/{age}")
  def putIntoKafka(@PathParam("topic") topic: String,
                   @PathParam("id") id: Int,
                   @PathParam("name") name: String,
                   @PathParam("age") age: Int) = tryJSONResponse {
    sendMessage(topic, id, name, age)
    ("insert succeed", null)
  }

  @GET
  @Path("/results")
  def get() = tryJSONResponse {
    ("get result succeed", getResults)
  }

  private def tryJSONResponse(f: => (String, Seq[String])): String = {
    import collection.JavaConverters._
    val response = new BaseResponse()
    val str = try {
      val (msg, data) = f
      response.setMsg(msg.toString)
      response.setResultCode(ErrorCode.success)
      if (data == null || data.isEmpty)
        JSON.toJSONString(response, true)
      else {
        val obj = new JSONObject
        val map = new util.HashMap[String, Object]()
        map.put("response", response)
        map.put("data", data.asJavaCollection)
        obj.fluentPutAll(map)
        obj.toJSONString
      }
    } catch {
      case e: Throwable =>
        response.setMsg(e.getCause.getMessage)
        response.setResultCode(ErrorCode.error)
        JSON.toJSONString(response, true)
    }
    str
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
}
