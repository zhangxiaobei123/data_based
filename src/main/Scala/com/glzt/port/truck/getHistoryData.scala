package com.glzt.port.truck


import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.json4s._
import org.json4s.jackson.JsonMethods._

object getHistoryData {
  def main(args: Array[String]): Unit = {

//     获取 Token
//    def getToken(secret: String, key: String): Map[String, Any] = {
//      // 创建 JSON 对象
//      val requestJson = s"""{"Secret":"$secret","Key":"$key"}"""
//
//      // 发送 HTTP POST 请求
//      val url = "https://chelian.gpskk.com/Data/GZIP/GetToken"
//      val request = new HttpPost(url)
//      request.addHeader("Content-Type", "application/json")
//      request.addHeader("Accept-Encoding", "gzip,deflate")
//      request.setEntity(new StringEntity(requestJson))
//      val client = HttpClientBuilder.create().build()
//      val response = client.execute(request)
//      val entity = response.getEntity
//      val result = parse(entity.getContent)
//
//      // 解析响应内容，并提取凭证对象
//      implicit val formats = DefaultFormats
//      val token = (result \ "Token").extract[Map[String, Any]]
//      // 返回凭证内容
//      token
//    }
//
//    // 调用车辆历史轨迹查询接口并获取 Key
//    val token = getToken("952392e8-1b49-4152-a733-3f7895d80de2", "0eef9431-4743-453a-98fe-88f0375421a6")
//    val vehicle = "浙 AP329R"
//    val from = "2019-06-05 10:46:54"
//    val to = "2019-06-06 10:46:54"
//    val requestJson =
//      s"""{"Temp_Token":"${token("Temp_Token")}",
//         | "Vehicle":"$vehicle",
//         | "From":"$from",
//         | "To":"$to"}""".stripMargin
//    val url = "https://chelian.gpskk.com/Data/GZIP/getHistoryData"
//    val request = new HttpPost(url)
//    request.addHeader("Content-Type", "application/json")
//    request.addHeader("Accept-Encoding", "gzip,deflate")
//    request.setEntity(new StringEntity(requestJson))
//    val client = HttpClientBuilder.create().build()
//    val response = client.execute(request)
//    val entity = response.getEntity
//    val result = parse(entity.getContent)
//    val code = (result \ "Code").extract[String]
//    val key = (result \ "Key").extract[String]
//    val msg = (result \ "Msg").extract[String]
//    println(s"Code: $code")
//    println(s"Key: $key")
//    println(s"Msg: $msg")







    // 定义方法
    def getHistoryData(vehicle: String, from: String, to: String): Option[String] = {
          def getToken(secret: String, key: String): Map[String, Any] = {
            // 创建 JSON 对象
            val requestJson = s"""{"Secret":"$secret","Key":"$key"}"""

            // 发送 HTTP POST 请求
            val url = "https://chelian.gpskk.com/Data/GZIP/GetToken"
            val request = new HttpPost(url)
            request.addHeader("Content-Type", "application/json")
            request.addHeader("Accept-Encoding", "gzip,deflate")
            request.setEntity(new StringEntity(requestJson))
            val client = HttpClientBuilder.create().build()
            val response = client.execute(request)
            val entity = response.getEntity
            val result = parse(entity.getContent)

            // 解析响应内容，并提取凭证对象
            implicit val formats = DefaultFormats
            val token = (result \ "Token").extract[Map[String, Any]]
            // 返回凭证内容
            token
          }
      val token = getToken("15023d3d-a721-497f-9119-3f88e63e614a", "897f078f-9e1c-4eeb-9e42-9ddd8f76f72e")
      // 调用车辆历史轨迹查询接口并获取 Key
      val requestJson = s"""{"Temp_Token":"${token("Temp_Token")}", "Vehicle":"$vehicle", "From":"$from", "To":"$to"}"""
      val url = "https://chelian.gpskk.com/Data/GZIP/getHistoryData"
      val request = new HttpPost(url)
      request.addHeader("Content-Type", "application/json")
      request.addHeader("Accept-Encoding", "gzip,deflate")
      request.setEntity(new StringEntity(requestJson))
      val client = HttpClientBuilder.create().build()
      val response = client.execute(request)
      val entity = response.getEntity
      val result = parse(entity.getContent)
      val key = (result \ "Key").extractOpt[String]
      key
    }

    // 调用方法
    val key = getHistoryData("川AEW686", "2022-04-08 23:59:59", "2019-06-06 10:46:54")
     println(s"Key:$key")



  }

}


import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.json4s._
import org.json4s.jackson.JsonMethods._

// 定义 case class





