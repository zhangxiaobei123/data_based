
import cn.hutool.http.{HttpRequest, Method}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, from_unixtime, substring, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.control.Breaks
import scala.util.matching.Regex

/***
 * 循环导入 温江区控站点接口数据到hive
 * 已跑完
 */

object WenJiangData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("温江区控数据入hive")
      .config("hive.exec.dynamici.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.merge.size.per.task", "256000000")
      .config("hive.merge.smallfiles.avgsize", "134217728")
      .config("hive.merge.mapfiles", "true")
      .config("hive.merge.mapredfiles", "false")
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")


 //  接口数据地址
        val AUTH: String = "basic RitqUFNveHFZa24xbGhkVWZhTFhjdDAxaTRyTEVWS0FFempEZDB5NlRqL0RmNW03Z mNJN1dxK2tGUExXYWhvSzBMTVZSWDlNcVpuZlZuSHhjR1kwelE9PQ=="
        val URL: String = "http://183.221.79.222:8003/WebApi6.0/api/MData/range?"

//  读取mysql   alpha-center 库的  aq_fixed_station
        val reader = spark.read.format("jdbc")
          //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
          .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "glzt-pro-bigdata")
          .option("password", "Uhh4QxUwiMsQ4mK4")
          .option("dbtable", "aq_fixed_station")

//  过滤掉 district_code='510115 的站点数据
        val source2: DataFrame = reader.load().filter("district_code='510115'")
//  取aq_fixed_station表中的 selectExpr ("station_name", "grid_id", "station_code", "station_level", "coord") 字段
        val airQualityStations = source2.selectExpr("station_name", "grid_id", "station_code", "station_level", "coord")
//
        //每小时执行一次
        def Date2Format(time: String): String = {
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val date: String = sdf.format(new Date((time.toLong)))
          date
        }

        val udf_times = udf((TimePoint: String) => {
          val pattern = new Regex("[0-9]")
          val published_at2 = (pattern findAllIn TimePoint).mkString("")
          val published_at = Date2Format(published_at2)
          published_at
        })

        val udf_null = udf((s: Any) => null)


     // 如果值等于—返回null  否者返回colmumn原来的值
        val udf_pdnull = udf((colmumn: String) =>
          if (colmumn == "—") {
            val cols = null
            cols
          }
          else {
            val cols: String = colmumn
            cols
          }
        )

//        def datafarme_insert(dataFrame: DataFrame) {
//          dataFrame.repartition(1)
//            .write.mode(SaveMode.Append)
//            .insertInto("ods_air.ods_port_station_hour")
//        }


    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }

    def getBetweenDates(start_time: String,end_time: String,end_time1:String) = {
    //解析
      var a = 0
      val loop = new Breaks;
      loop.breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)
          val end1 = getHoursTime(end_time, a)

          println(start1)
          // 接口 连接
          val requestUrl: String = URL + s"stationIds=%20%20&reportTimeType=4&appType=0&startTime=$start1&endTime=$end1&workCondition=2&rankField=TimePoint&getOtherNodes=True&stationLevel=4"
          //发送请求 失败重试5次 超时时间为3秒钟

          var body: String = null
          val loop = new Breaks;
          loop.breakable {
            for (i <- 0 until 5) {
              val response = new HttpRequest(requestUrl).method(Method.GET)

                .header("www-authenticate", AUTH).timeout(3 * 1000).execute
              if (response.getStatus == 200) {
                body = response.body
                loop.break
              }
              else {
                println("站点小时空气质量请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
              }
            }
          }
          //解析接口获取的数据为JSON
          val result = JSON.parseObject(body)
          //  接口数据
          val data = result.getJSONArray("data").toString
           // 打印接口数据
          println(data)
          import spark.implicits._
//          转换成DF
          val msgdf = Seq(data).toDF("wenJiang")
//            创建临时表
          msgdf.createOrReplaceTempView("wenJiang_table")
//          将接口数据写入临时表中
          val jsonDF = spark.sql("select wenJiang from wenJiang_table")
//          获取接口数据转换为RDd
          val rdd = jsonDF.rdd.map(_.getString(0))
//          读取里面数据解析成DS 并分拆字段
          val wenJiang_aqi = spark.read.json(rdd.toDS())
            .withColumnRenamed("UniqueCode", "station_code")
            .withColumnRenamed("TimePoint", "published_at")
            .withColumn("published_at",substring(col("published_at"), 7, 13)/1000)  // 2023-02-02 12:00:00
            .withColumn("published_at",from_unixtime(col("published_at"), "yyyy-MM-dd HH:00:00"))
            .withColumnRenamed("CO", "co")
            .withColumn("co", udf_pdnull(col("co")))
            .withColumnRenamed("O3", "o3")
            .withColumn("o3", udf_pdnull(col("o3")))
            .withColumnRenamed("PM2_5", "pm2_5")
            .withColumn("pm2_5", udf_pdnull(col("pm2_5")))
            .withColumnRenamed("NO2", "no2")
            .withColumn("no2", udf_pdnull(col("no2")))
            .withColumnRenamed("SO2", "so2")
            .withColumn("so2", udf_pdnull(col("so2")))
            .withColumnRenamed("PM10", "pm10")
            .withColumn("pm10", udf_pdnull(col("pm10")))
            .withColumn("aqi", udf_null(col("pm2_5")))
            .withColumn("wind_direction", udf_null(col("pm2_5")))
            .withColumn("wind_power", udf_null(col("pm2_5")))
            .withColumn("temperature", udf_null(col("pm2_5")))
            .withColumn("pressure", udf_null(col("pm2_5")))
            .withColumn("humidity", udf_null(col("pm2_5")))
//            获取published_at 的年月日作为分区字段
            .withColumn("publish_date", substring(col("published_at"), 0, 10))
//            和mysql库的aq_fixed_station按照station_code进行leftJoin
            .join(airQualityStations,Seq("station_code"), "left")
            .withColumnRenamed("station_level", "station_type")
//            获取如下字段.selectExpr("station_name", "coord", "no2", "o3", "pm2_5", "so2", "pm10", "aqi", "co", "grid_id", "station_code", "station_type", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure", "publish_date")
              .selectExpr("station_name", "coord", "no2", "o3", "pm2_5", "so2", "pm10", "aqi", "co", "grid_id", "station_code", "station_type", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure", "publish_date")

//            wenJiang_aqi.show()  展示数据
//          datafarme_insert(wenJiang_aqi)   写入没有表中
          a = a + 1
          if (start1 == end_time1) {
            loop.break
          }
        }
      }
  }

//     多线程 提交资源
//    def run1()(implicit xc: ExecutionContext) = Future {
//      getBetweenDates("2022-03-08 00:00:00","2022-03-08 02:00:00","2022-03-31 23:00:00")
//    }
//
//    def run2()(implicit xc: ExecutionContext) = Future {
//      getBetweenDates("2022-06-08 00:00:00","2022-06-08 02:00:00","2022-06-30 23:00:00")
//    }

    def run3()(implicit xc: ExecutionContext) = Future {
      getBetweenDates("2022-09-11 00:00:00","2022-09-11 02:00:00","2022-09-11 02:00:00")
    }

//    def run4()(implicit xc: ExecutionContext) = Future {
//     getBetweenDates("2022-12-06 00:00:00","2022-12-06 02:00:00","2022-12-31 23:00:00")
//    }

    val pool = Executors.newFixedThreadPool(1)
    implicit val xc = ExecutionContext.fromExecutorService(pool)
//    val task1 = run1()
//    val task2 = run2()
    val task3 = run3()
//    val task4 = run4()

//    Await.result(Future.sequence(Seq(task1, task2, task3, task4)), Duration(999,duration.DAYS))
    Await.result(Future.sequence(Seq(task3)), Duration(999,duration.DAYS))
    pool.shutdown()

  }
}
