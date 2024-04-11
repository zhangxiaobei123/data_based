



import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.control.Breaks.break

/***
 *
 *   将 trucksTrack 文件追加到hive表运渣车接口数据表中
 *
 */
object Test {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("hive")
      .config("hive.exec.dynamici.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val x_PI = 3.14159265358979324 * 3000.0 / 180.0
    val PI = 3.1415926535897932384626
    val a = 6378245.0
    val ee = 0.00669342162296594323


    def transform_WGS84_to_GJJCJ02(lon: Double, lat: Double): Map[String, Double] = {
      if (out_of_china(lon, lat))
        Map[String, Double](
          "lon" -> lon,
          "lat" -> lat
        )
      else {
        var d_lat = transform_lat(lon - 105.0, lat - 35.0)
        var d_lon = transform_lon(lon - 105.0, lat - 35.0)
        val red_lat = lat / 180.0 * PI
        var magic = Math.sin(red_lat)
        magic = 1 - ee * magic * magic
        val sqrtMagic = Math.sqrt(magic)
        d_lat = (d_lat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * PI)
        d_lon = (d_lon * 180.0) / (a / sqrtMagic * Math.cos(red_lat) * PI)
        Array[Double](lon + d_lon, lat + d_lat)
        Map[String, Double](
          "lon" -> (lon + d_lon),
          "lat" -> (lat + d_lat)
        )
      }
    }

    def out_of_china(lon: Double, lat: Double) = (lon < 72.004 || lon > 137.8347) || (lat < 0.8293 || lat > 55.8271)


    def transform_lat(lon: Double, lat: Double) = {
      var ret = -100.0 + 2.0 * lon + 3.0 * lat + 0.2 * lat * lat + 0.1 * lon * lat + 0.2 * Math.sqrt(Math.abs(lon))
      ret += (20.0 * Math.sin(6.0 * lon * PI) + 20.0 * Math.sin(2.0 * lon * PI)) * 2.0 / 3.0
      ret += (20.0 * Math.sin(lat * PI) + 40.0 * Math.sin(lat / 3.0 * PI)) * 2.0 / 3.0
      ret += (160.0 * Math.sin(lat / 12.0 * PI) + 320 * Math.sin(lat * PI / 30.0)) * 2.0 / 3.0
      ret
    }

    /**
     * 转换经度
     * */

    def transform_lon(lon: Double, lat: Double) = {
      var ret = 300.0 + lon + 2.0 * lat + 0.1 * lon * lon + 0.1 * lon * lat + 0.1 * Math.sqrt(Math.abs(lon))
      ret += (20.0 * Math.sin(6.0 * lon * PI) + 20.0 * Math.sin(2.0 * lon * PI)) * 2.0 / 3.0
      ret += (20.0 * Math.sin(lon * PI) + 40.0 * Math.sin(lon / 3.0 * PI)) * 2.0 / 3.0
      ret += (150.0 * Math.sin(lon / 12.0 * PI) + 300.0 * Math.sin(lon / 30.0 * PI)) * 2.0 / 3.0
      ret
    }



    val get_raw_lon = udf((lon: Double, lat: Double) => {
      transform_WGS84_to_GJJCJ02(lon, lat).get("lon").get.formatted("%.6f")
    })

    val get_raw_lat = udf((lon: Double, lat: Double) => {
      transform_WGS84_to_GJJCJ02(lon, lat).get("lat").get.formatted("%.6f")
    })


    def getDateTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.DATE, +int)
      val time = df.format(calendar.getTime)
      time
    }


//    def getBetweenDates(start_time: String, end_time: String) = {
//      //解析
//      var a = 0
//      while (true) {
//        val start1 = getDateTime(start_time, a)
//        val end1 = getDateTime(end_time, a)
//        println(start1)
//          try {
//            spark.read.parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/trucksTrack/track/uptime=$start1")
//              // dropDuplicates 去掉重复的行
//              .dropDuplicates("plate_num", "up_time")
//              .withColumnRenamed("point_x", "raw_lon")
//              .withColumnRenamed("point_y", "raw_lat")
//              .withColumn("lon", get_raw_lon(col("raw_lon"), col("raw_lat")))
//              .withColumn("lat", get_raw_lat(col("raw_lon"), col("raw_lat")))
//              .withColumn("publish_date", substring(from_unixtime(col("up_time")), 0, 10))
//              .withColumn("hour", from_unixtime(col("up_time")).substr(12, 2))
//              .selectExpr("plate_num", "sim_card", "direction", "speed", "up_time", "raw_lon", "raw_lat", "lon", "lat", "publish_date", "hour")
//              .coalesce(1)
//              .write
//              .mode(SaveMode.Append)
//              .insertInto("ods_traffic.ods_slag_truck_interface")
//          } catch {
//            case e: Exception => println(s"Data not found for $start1")
//          }
//        a = a + 1
//        if (start1 == end_time) {
//          break
//        }
//      }
//    }

//    getDateTime("2022-07-25","2022-08-22")




  }
}
