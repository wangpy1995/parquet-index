package com.service.response

import javax.xml.bind.annotation.XmlRootElement

import scala.beans.BeanProperty

@XmlRootElement
class BaseResponse {
  @BeanProperty var resultCode:Int = _
  @BeanProperty var msg:String = _
}
