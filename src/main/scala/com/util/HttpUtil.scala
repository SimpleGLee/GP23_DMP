package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 10-31
  * Http请求协议    Get请求
  */
object HttpUtil {
  /**
    * Get请求
    * @param url
    * @return Json
    */
  def get(url:String):String = {
    //使用默认的请求协议
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //获取发送请求
    val httpResponse: CloseableHttpResponse = client.execute(httpGet)
    //处理乱码处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")

  }

}
