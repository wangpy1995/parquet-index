package test.scala

object SparkSubmit {

  def submit(mode:String)={
    val server = new Server()
    val connector = new SelectChannelConnector()
    connector.setPort(HTTP_PORT)
    server.addConnector(connector)
    val handler = new SparkJobSubmitHandler
    server.setHandler(handler)

    server.start()
    server.join()
  }

}


