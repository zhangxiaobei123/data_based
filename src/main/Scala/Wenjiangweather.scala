
import cn.hutool.http.{HttpRequest, Method}

import java.text.SimpleDateFormat
import java.util.Calendar
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, instr, substring, to_date, udf}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks


object Wenjiangweather {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("my local spark")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //      .config("hive.exec.dynamici.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .config("hive.merge.size.per.task", "256000000")
//      .config("hive.merge.smallfiles.avgsize", "134217728")
//      .config("hive.merge.mapfiles", "true")
//      .config("hive.merge.mapredfiles", "false")
//      .enableHiveSupport()
//      .getOrCreate()
//    val sc: SparkContext = spark.sparkContext
//    sc.setLogLevel("ERROR")


    val AUTH: String = "basic RitqUFNveHFZa24xbGhkVWZhTFhjdDAxaTRyTEVWS0FFempEZDB5NlRqL0RmNW03Z mNJN1dxK2tGUExXYWhvSzBMTVZSWDlNcVpuZlZuSHhjR1kwelE9PQ=="
    val URL: String = "http://111.9.55.200:10000/cimiss-web/api?"

//    val reader = spark.read.format("jdbc")
//      //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
//      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "glzt-pro-bigdata")
//      .option("password", "Uhh4QxUwiMsQ4mK4")
//      .option("dbtable", "aq_fixed_station")

//    val source2: DataFrame = reader.load().filter("district_code='510115'")
//    //过滤站点
//    val airQualityStations = source2.selectExpr("station_name", "grid_id", "station_code", "station_level", "coord")


    //每小时执行一次
//    def Date2Format(time: String): String = {
//      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val date: String = sdf.format(new Date((time.toLong)))
//      date
//    }
//
//    val udf_times = udf((TimePoint: String) => {
//      val pattern = new Regex("[0-9]")
//      val published_at2 = (pattern findAllIn TimePoint).mkString("")
//      val published_at = Date2Format(published_at2)
//      published_at
//    })

    val udf_null = udf((s: Any) => null)

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

//    def datafarme_insert(dataFrame: DataFrame) {
//      dataFrame.repartition(1)
//        .write.mode(SaveMode.Append)
//        .insertInto("ods_air.ods_port_station_hour")
//    }


    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyyMMddHHmmss")

      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }
    def getBetweenDates(start_time: String, end_time: String) = {
      //解析
      var a = 0
      val loop = new Breaks;
      loop.breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)

          println(start1)
          val requestUrl: String = URL + s"dataFormat=json&interfaceId=getSurfEleByTimeRangeAndStaID&dataCode=SURF_CHN_MUL_HOR&elements=Station_Name,City,Cnty,Datetime,Station_Id_C,Admin_Code_CHN,PRE_1h,PRS,RHU,TEM,WIN_D_INST_Max,WIN_S_Inst_Max,DUANXIN_STATION_NAME,GONGFU_STATION_NAME,HUITU_STATION_NAME&limitCnt=100&userId=wjq_qxj&pwd=d2a8054d259091babccf11c6013c9df7b4f9d8a685c0c58717435658889e11e6&staIds=S1018,S1701,S1702,S1704,S1707,S1708,S1709,S1710,S1711,S1712,S1713,S1720,56187&timeRange=[$start1,$end_time]"
          //发送请求 失败重试5次 超时时间为3秒钟

          var body: String = null
          val loop = new Breaks
          loop.breakable {
            for (i <- 0 until 5) {
              val response = new HttpRequest(requestUrl).method(Method.GET).header("www-authenticate", AUTH).timeout(3 * 1000).execute
              if (response.getStatus == 200) {
                body = response.body
                loop.break
              }
              else {
                println("气象站点小时空气质量请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
              }
            }
          }

          val result = JSON.parseObject(body)
          val data = result.getJSONArray("DS").toString
                    println(data)
          import spark.implicits._
          val msgdf = Seq(data).toDF("wenJiang")
          msgdf.createOrReplaceTempView("wenJiang_table")
          val jsonDF = spark.sql("select wenJiang from wenJiang_table")

          val rdd = jsonDF.rdd.map(_.getString(0))
          val wenJiang_aqi = spark.read.json(rdd.toDS())
            .withColumn("STATION_ID_C",col("STATION_ID_C"))
            .withColumn("STATION_NAME",col("STATION_NAME"))
            .withColumn("CITY",col("CITY"))
            .withColumn("CNTY",col("CNTY"))
            .withColumn("DATETIME",col("DATETIME"))
            .withColumn("ADMIN_CODE_CHN",col("ADMIN_CODE_CHN"))
            .withColumn("PRE_1H",col("PRE_1H"))
            .withColumn("PRS",col("PRS"))
            .withColumn("RHU",col("RHU"))
            .withColumn("TEM",col("TEM"))
            .withColumn("WIN_D_INST_MAX",col("WIN_D_INST_MAX"))
            .withColumn("WIN_S_INST_MAX",col("WIN_S_INST_MAX"))
            .withColumn("DUANXIN_STATION_NAME",col("DUANXIN_STATION_NAME"))
            .withColumn("GONGFU_STATION_NAME",col("GONGFU_STATION_NAME"))
            .withColumn("HUITU_STATION_NAME",col("HUITU_STATION_NAME"))
            .withColumn("publish_date", substring(col("DATETIME"), 0, 10))
            .selectExpr("STATION_ID_C", "STATION_NAME", "CITY", "CNTY", "DATETIME", "ADMIN_CODE_CHN", "PRE_1H", "PRS", "RHU", "TEM", "WIN_D_INST_MAX", "WIN_S_INST_MAX", "DUANXIN_STATION_NAME", "GONGFU_STATION_NAME", "HUITU_STATION_NAME", "publish_date")
            wenJiang_aqi.show()
//          datafarme_insert(wenJiang_aqi)
          a = a + 1
          if (start1 == end_time) {
            loop.break
          }
        }
      }
    }
    getBetweenDates("20230202000000","20230202230000")
  }

}
