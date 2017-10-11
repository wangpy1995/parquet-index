package server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.webapp.WebAppContext

object TaskServer {
  val HTTP_PORT = 8080
  val CONTEXT = "/"
  val DEFAULT_APP_PATH = "src/main/webapp"

  def main(args: Array[String]): Unit = {
    val server = new Server()

    val connector = new SelectChannelConnector()
    connector.setPort(HTTP_PORT)

    val webAppContext = new WebAppContext(CONTEXT, DEFAULT_APP_PATH)
    webAppContext.setDescriptor("web/WEB-INF/web.xml")
    webAppContext.setResourceBase(DEFAULT_APP_PATH)
    webAppContext.setClassLoader(Thread.currentThread().getContextClassLoader)
    server.addConnector(connector)
    server.setHandler(webAppContext)

    server.start()
    server.join()
  }
}
