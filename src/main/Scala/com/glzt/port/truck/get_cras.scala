package com.glzt.port.truck




import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.JsonMethods.compact




object get_cras {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      //      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //      .config("hive.exec.dynamic.partition", "true")
//      .enableHiveSupport()
      .getOrCreate()


    def getToken(secret: String, key: String): Map[String, Any] = {
      // 定义请求参数
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

      // 解析响应内容，并提取凭证对象
      implicit val formats = DefaultFormats
      val token = (result \ "Token").extract[Map[String, Any]]
      // 返回凭证内容
      token
    }

    val token = getToken("952392e8-1b49-4152-a733-3f7895d80de2", "0eef9431-4743-453a-98fe-88f0375421a6")
    val tempToken = s"${token("Temp_Token")}"
    val carrequestBody = s"""{"Temp_Token":"$tempToken"}"""
    val carsurl = "https://chelian.gpskk.com/Data/GZIP/GetCars"
    val carrequest = new HttpPost(carsurl)
    carrequest.addHeader("Content-Type", "application/json")
    carrequest.addHeader("Accept-Encoding", "gzip,deflate")
    carrequest.setEntity(new StringEntity(carrequestBody))
    val client = HttpClientBuilder.create().build()
    val response = client.execute(carrequest)
    val entity = response.getEntity
    val result = parse(entity.getContent)
//println(result)
    val carResult = (result \ "CarResult").children

    import spark.implicits._
    val jsonList = carResult.map(compact)
    val jsonDS = spark.createDataset(jsonList)
    val df = spark.read.json(jsonDS)
    val resDF = df.select(
      struct(df.columns.map(col): _*).alias("OtherInfo")
    ).withColumn("OtherInfo", to_json(col("OtherInfo")))
      .select(
        get_json_object($"OtherInfo", "$.CarBrand").alias("CarBrand"), // 新增取出 CarBrand 的代码
        $"OtherInfo"
      )
      .drop("OtherInfo.CarBrand")


    resDF.show()


    //    // 将 DataFrame 写入 MySQL 数据库
    //    val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true"
    //    val username = "zhangyaobin"
    //    val password = "cFrb$BBc8g6^mQBl"
    //    val props = new Properties()
    //    props.setProperty("user", username)
    //    props.setProperty("password", password)
    //    val table = "mytable"
    //
    //    val connection: Connection = DriverManager.getConnection(url, username, password)
    //    val statement: PreparedStatement = connection.prepareStatement("INSERT INTO " + table + "(CarBrand, OtherInfo) VALUES (?, ?)")
    //    resDF.foreach { row =>
    //      statement.setString(1, row.getAs[String]("CarBrand"))
    //      statement.setString(2, row.getAs[String]("OtherInfo"))
    //      statement.addBatch()
    //    }
    //    statement.executeBatch()
    //    statement.close()
    //    connection.close()
    //
    //    // 关闭 SparkSession
    //    spark.close()


    }
}
