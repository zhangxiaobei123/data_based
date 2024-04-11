import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions.{col, substring, udf}
import com.alibaba.fastjson.JSON

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.control.Breaks

object Wh_MicroStationData{
  def main(args: Array[String]): Unit = {

//      微站数据
      val spark = SparkSession
        .builder()
//        .master("local[*]")
        .appName("温江区控数据入hive")
        .config("hive.exec.dynamici.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.merge.size.per.task", "256000000")
        .config("hive.merge.smallfiles.avgsize", "134217728")
        .config("hive.merge.mapfiles", "true")
        .config("hive.merge.mapredfiles", "false")
        .enableHiveSupport()
        .getOrCreate()
//      val sc: SparkContext = spark.sparkContext
//      sc.setLogLevel("ERROR")


      def postResponse(url: String, params: String = null, header: String = null): String = {
        val httpClient = HttpClients.createDefault() // 创建 client 实例
        val post = new HttpPost(url) // 创建 post 实例
        // 设置 header
        var header =
          """
            |{"Content-Type": "application/json"}
            |
            |""".stripMargin
        if (header != null) {
          val json = JSON.parseObject(header)
          json.keySet().toArray.map(_.toString)
            .foreach(
                  key => post.setHeader(key, json.getString(key)))
          }
        if (params != null) {
          post.setEntity(new StringEntity(params, "UTF-8"))

        }
        val response = httpClient.execute(post) // 创建 client 实例
        val str = EntityUtils.toString(response.getEntity, "UTF-8")
        response.close()
        httpClient.close()
        str
      }


      val changtype = udf((stationtype: String) => {
        val stationtype = "micro"
        stationtype
      })

      val reader = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", "tancongjian")
        .option("password", "mK81VrWmFzUUrrQd")
        .option("dbtable", "aq_fixed_station")

      val source2 = reader.load()
      //过滤站点
      val airQualityStations = source2.selectExpr("station_name", "grid_id", "station_code", "station_level", "coord", "station_type").filter("district_code ='510107' and station_type='alphamaps_micro_station'")

      def getHoursTime(string: String, int: Int): String = {
        val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
        val calendar: Calendar = Calendar.getInstance()
        calendar.setTime(df.parse(string))
        calendar.add(Calendar.HOUR, +int)
        val time = df.format(calendar.getTime)
        time
      }

    def datafarme_insert(dataFrame: DataFrame) {
      dataFrame.repartition(1)
        .write.mode(SaveMode.Append)
        .insertInto("ods_air.ods_port_station_hour")
    }


      def getBetweenDates(start_time: String, end_time: String, end_time1: String) = {
        //解析
        var a = 0
        val loop = new Breaks;
        loop.breakable {
          while (true) {
            val start1 = getHoursTime(start_time, a)
            val end1 = getHoursTime(end_time, a)

//            println("Wh_MicroStation="+start1)
            // 连接接口
            val data = postResponse("http://47.108.167.64:8082/station/hour",
              s"""
                 |{"accessKey":"d13bb62a61d9420abb6f9fb1b7920a2b",
                 |"startDate":"$start1",
                 |"endDate":"$end1"}
                 |""".stripMargin,
              """
                |{"Content-Type": "application/json"}
                |""".stripMargin
            )
            // 获取接口中的数据
            val JSONObject = JSON.parseObject(data)
            println(data)
            val results = JSONObject.getJSONArray("result").toString()
            import spark.implicits._
            val msgdf = Seq(results).toDF("wh")
            msgdf.createOrReplaceTempView("wh_table")
            val jsonDF = spark.sql("select wh from wh_table")
            val rdd = jsonDF.rdd.map(_.getString(0))
            val wh_aqi = spark.read.json(rdd.toDS())
              .withColumnRenamed("stationId", "station_code")
              .withColumnRenamed("insTime", "published_at")
              .withColumnRenamed("pm25", "pm2_5")
              .withColumnRenamed("windDirection", "wind_direction")
              .withColumnRenamed("windPower", "wind_power")
              .join(airQualityStations, Seq("station_code"), "left")
              .withColumn("publish_date", substring(col("published_at"), 0, 10))
              .withColumn("station_type", changtype(col("station_type")))
              .drop("station_level")
              .selectExpr("station_name", "coord", "no2", "o3", "pm2_5", "so2", "pm10", "aqi", "co", "grid_id", "station_code", "station_type", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure", "publish_date")
//              datafarme_insert(wh_aqi)
            wh_aqi.show()
            a = a + 1
            if (start1 == end_time1) {
              loop.break
            }
          }
        }
      }

//    println(getBetweenDates("2022-11-27 00:00:00", "2022-01-27 02:00:00", "2022-01-27 01:00:00"))



    def run1()(implicit xc: ExecutionContext) = Future {
      getBetweenDates("2022-03-11 00:00:00", "2022-03-11 02:00:00", "2022-03-31 23:00:00")
    }
//
//    def run2()(implicit xc: ExecutionContext) = Future {
//      getBetweenDates("2022-06-03 00:00:00", "2022-06-03 02:00:00", "2022-06-30 23:00:00")
//    }
//
//    def run3()(implicit xc: ExecutionContext) = Future {
//      getBetweenDates("2022-09-01 00:00:00", "2022-09-01 02:00:00", "2022-09-30 23:00:00")
//    }
//
//    def run4()(implicit xc: ExecutionContext) = Future {
//      getBetweenDates("2022-12-03 00:00:00", "2022-12-03 02:00:00", "2022-12-31 23:00:00")
//    }
//
    val pool = Executors.newFixedThreadPool(1)
    implicit val xc = ExecutionContext.fromExecutorService(pool)
    val task1 = run1()
//    val task2 = run2()
//    val task3 = run3()
//    val task4 = run4()
//
//    Await.result(Future.sequence(Seq(task1, task2, task3, task4)), Duration(999, duration.DAYS))
    Await.result(Future.sequence(Seq(task1)), Duration(999, duration.HOURS))
    pool.shutdown()






    }


}
