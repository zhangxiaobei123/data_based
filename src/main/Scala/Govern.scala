
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}

import scala.util.control.Breaks.break

/** *
 *
 * enableHiveSupport ()读取配置文件
 *
 * 小文件合并 ： 先创建临时表合并到hdfs 再写回obs 可以保证数据不丢失
 */
object Govern {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      //      .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      .getOrCreate()

    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.DATE, +int)
      val time = df.format(calendar.getTime)
      time
    }


    //         // 读取 parquet文件
    //    val df = spark.read.parquet(s" obs://bigdata-glzt/datahouse/water/ods/PRO_DATA/SYD_BASIC_INFO/fdc84cf6-8b23-4627-a0f1-de6f3d0d16cd.parquet").show()

    //  show()不穿参 默认前十条数据
    //    df.show()

    //    println(df.count())  查看数据量

    //      // 执行sql 读取 published_date='2022-08-15 分区数据
    //    spark.sql("select * from dwd_construction_site.dwd_construction_site_cd_hour_dust where published_date='2022-08-15'").show()

    //    spark.sql("select * from ods_water.water_syd_baseinfo_65").show()
    //    spark.sql("REFRESH TABLE ods_traffic.ods_slag_truck_interface")   // 刷新数据

    //  读取表中数据
    //    spark.sql("select * from dwd_enterprise.dwd_ent_hour_power_consumption").show()

    // 添加分区
    //    alter table ods_air.ods_port_station_hour add if not exists partition(dt='2022-01-02')

    //    复制表结构 默认 hdfs
    //    spark.sql("create table ods_traffic.ods_slag_truck_interface_test1 like ods_traffic.ods_slag_truck_interface")

    //        两种方法一样 都是截取日期字段的年月日 作为分区字段
    //      .withColumn("published_date",col("published_at").substr(0,10))
    //      .withColumn("publish_date", substring(col("published_at"), 0, 10))

    //  截取小时字段作为 二层分区
    //      .withColumn("hour", from_unixtime(col("published_at")).substr(12, 2))


    /*

       读取obs://bigdata-glzt/datahouse/air/ods/ods_air/ods_port_station_hour 下的parquet文件写入
      hdfs://hacluster/user/hive/warehouse/ods_air.db/ods_port_station_hour_test1/publish_date=$start1
       或者写入到表中 .insertInto("dwd_construction_site.dwd_construction_site_cd_hour_dust")

*/
    //    val list1 = List("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
    //    for(i1 <- list) {
    //      for (i <- list1) {
    //         val df = spark.read.parquet(s"obs://bigdata-glzt/datahouse/air/ods/ods_air/ods_port_station_hour/publish_date=$start1")
    //          .coalesce(1)
    //          .write
    //          .mode(SaveMode.Overwrite)
    //          .parquet(s"hdfs://hacluster/user/hive/warehouse/ods_air.db/ods_port_station_hour_test1/publish_date=$start1")
    //      }
    //    }
    //    //
    //
    //
    //

    /** *
     * 文件合并写入临时表路径
     *
     * @param start_time 开始时间
     * @param end_time   结束时间
     *                   根据时间 开始循环写入  写出数据
     * */


    def getBetweenDates(start_time: String, end_time: String): Unit = {
      //解析
      var a = 0
      while (true) {
        val start1 = getHoursTime(start_time, a)
        println(start1)
        val list = List("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
        for (i <- list) {
          try {
            val df = spark.read.parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=$start1/hour=$i")
              .coalesce(1)
              .write
              .mode(SaveMode.Append)
              .parquet(s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=$start1/hour=$i")
          } catch {
            case e: Exception => println(s"Data not found for $start1")
          }
          a = a + 1
          if (start1 == end_time) {
            break
          }
        }
      }
    }

    /** *
     * 读取 hdfs文件写回去ods_slag_truck_interface文
     *
     * @param start_time
     * @param end_time
     */
    def getBetweenDates1(start_time: String, end_time: String): Unit = {
      //解析
      var a = 0
      while (true) {
        val start1 = getHoursTime(start_time, a)
        println(start1)
        val list = List("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
        for (i <- list) {
          try {
            val df = spark.read.parquet(s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=$start1/hour=$i")
              .coalesce(1)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=$start1/hour=$i")
          } catch {
            case e: Exception => println(s"Data not found for $start1")
          }
          a = a + 1
          if (start1 == end_time) {
            break
          }
        }
      }
    }







    /*
val list1 = List("03", "04", "05", "06", "07", "08", "09", "10")
//val list1 = List( "30", "31")
for (j <- list1) {
val list = List("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
for (i <- list) {
    //            val df = spark.read.parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=2023-04-$j/hour=$i")
    //              .coalesce(1)
    //              .write
    //              .mode(SaveMode.Append)
    //              .parquet(s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=$start1/hour=$i")
    val df = spark.read.parquet(s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=2023-04-$j/hour=$i")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=2023-04-$j/hour=$i")
  }
}*/
  }
}