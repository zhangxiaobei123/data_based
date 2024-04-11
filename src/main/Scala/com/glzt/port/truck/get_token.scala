package com.glzt.port.truck

import org.apache.spark.sql.SparkSession
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.StringEntity
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.json4s.DefaultFormats


object get_token {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .config("hive.exec.dynamic.partition", "true")
//      .enableHiveSupport()
      .getOrCreate()

    // 定义请求参数
    val secret = "952392e8-1b49-4152-a733-3f7895d80de2"
    val key = "0eef9431-4743-453a-98fe-88f0375421a6"
    val requestBody = compact(parse(s"""{"Secret":"$secret","Key":"$key"}"""))

    // 构建 HTTP POST 请求
    val url = "https://chelian.gpskk.com/Data/GZIP/GetToken"
    val request = new HttpPost(url)
    request.addHeader("Content-Type", "application/json")
    request.addHeader("Accept-Encoding", "gzip,deflate")
    request.setEntity(new StringEntity(requestBody))

    // 发送请求，并获取响应内容
    val client = HttpClientBuilder.create().build()
    val response = client.execute(request)
    val entity = response.getEntity
    val result = parse(entity.getContent)
    println(result)

    // 解析响应内容，并提取凭证对象
    implicit val formats = DefaultFormats
    val token = (result \ "Token").extract[Map[String, Any]]

    // 输出凭证内容
    println(s"Expires_In: ${token("Expires_In")}")
    println(s"Temp_Token: ${token("Temp_Token")}")
    println(s"UpdateTime: ${token("UpdateTime")}")



  }





}
