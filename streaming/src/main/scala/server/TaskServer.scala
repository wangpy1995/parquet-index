package server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.webapp.WebAppContext

object TaskServer {
  val HTTP_PORT = 8081
  val CONTEXT = "webContent"
  val DEFAULT_APP_PATH ="streaming/web"

  def startServer()={
    val server = new Server()

    val connector = new SelectChannelConnector()
    connector.setPort(HTTP_PORT)

    val webAppContext = new WebAppContext()
    webAppContext.setDescriptor(DEFAULT_APP_PATH+"/WEB-INF/web.xml")
    webAppContext.setResourceBase(DEFAULT_APP_PATH)
    webAppContext.setContextPath("/")
    webAppContext.setClassLoader(Thread.currentThread().getContextClassLoader)
    server.addConnector(connector)
    server.setHandler(webAppContext)

    server.start()
    server.join()
  }

  def main(args: Array[String]): Unit = {
    startServer()
  }
}
