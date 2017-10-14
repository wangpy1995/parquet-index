package service.response

import javax.xml.bind.annotation.XmlRootElement

import scala.beans.BeanProperty

@XmlRootElement
class BaseResponse {
  @BeanProperty var resultCode = 0
  @BeanProperty var msg = "success"
}
