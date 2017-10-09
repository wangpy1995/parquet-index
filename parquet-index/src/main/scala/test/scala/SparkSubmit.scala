package test.scala

import java.util.UUID
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.jetty.http.{HttpHeaders, HttpMethods, MimeTypes}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.{AbstractHttpConnection, Request, Server}

object SparkSubmit {

  private val HTTP_PORT = 8080

  def submit(mode: String) = {
    val sparkConf = new SparkConf().setMaster(mode).setAppName(s"jetty_${UUID.randomUUID()}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val server = new Server()
    val connector = new SelectChannelConnector()
    connector.setPort(HTTP_PORT)
    server.addConnector(connector)
    val handler = new SparkJobSubmitHandler(sc)
    server.setHandler(handler)

    server.start()
    server.join()
  }

  def main(args: Array[String]): Unit = {
    submit("yarn")
  }

}

class SparkJobSubmitHandler(sc: SparkContext) extends AbstractHandler {
  private val _cacheControl = "must-revalidate,no-cache,no-store"

  private val str = "ds,ds,ds,sd,sc"

  private def test(words: String) = sc.parallelize(words.split(",")).map((_, 1))
    .reduceByKey(_ + _).collect()

  override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) = {
    val connection = AbstractHttpConnection.getCurrentConnection
    connection.getRequest.setHandled(true)
    val method = request.getMethod
    if (!(method == HttpMethods.GET) && !(method == HttpMethods.POST) && !(method == HttpMethods.HEAD))
      response.sendError(-1, s"request method error:[$method] ")
    response.setContentType(MimeTypes.TEXT_HTML_UTF_8)
    if (_cacheControl != null) response.setHeader(HttpHeaders.CACHE_CONTROL, _cacheControl)
    response.getOutputStream.write(test(str).mkString("<br/>").getBytes())
  }
}


