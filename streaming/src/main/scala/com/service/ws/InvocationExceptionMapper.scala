package com.service.ws

import java.util.Locale
import javax.ws.rs.core.Response
import javax.ws.rs.ext.ExceptionMapper

import com.google.gson.GsonBuilder
import com.service.response.BaseResponse
import org.slf4j.LoggerFactory
import com.service._


class InvocationExceptionMapper extends ExceptionMapper[Throwable] {
  private val LOG = LoggerFactory.getLogger(classOf[InvocationExceptionMapper])
  /**
    * 用于拦截rest方法抛出的异常，返回给客户端。
    */
    override def toResponse(ex: Throwable): Response = {
      LOG.error("rest interface invoke failed", ex)
      val trace = new Array[StackTraceElement](1)
      trace(0) = ex.getStackTrace()(0)
      ex.setStackTrace(trace)
      val result = new BaseResponse
      val msg = "rest interface invoke failed"
      result.setMsg(msg)
      result.setResultCode(ErrorCode.error)
      val json = new GsonBuilder().disableHtmlEscaping.create.toJson(result)
      val rb = Response.ok(json)
      rb.`type`("application/json;charset=UTF-8")
      rb.language(Locale.SIMPLIFIED_CHINESE)
      rb.build
    }
}