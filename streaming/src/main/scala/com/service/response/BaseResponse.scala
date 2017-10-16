package com.service.response

import javax.xml.bind.annotation.XmlRootElement

import scala.beans.BeanProperty

@XmlRootElement
class BaseResponse {
  @BeanProperty var msg: String = _
  @BeanProperty var resultCode: Int = _
}
