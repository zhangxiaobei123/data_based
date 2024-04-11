package com.glzt.ods


import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.Try
import scala.util.control.Breaks

/** *
 * 小文件合并 写入目标表
 */


object Truck_write_obs {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()


    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }


    def getBetweenDates(start_time: String, end_time: String): Unit = {
      val breaks = new Breaks
      var a = 0
      breaks.breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)
          val start2 = start1.substring(0, 10)
          val i = start1.substring(11, 13)
          println(start2)

          val path = s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=$start2/hour=$i"
          if (Try(spark.read.parquet(path)).isSuccess) {
            val df = spark.read.parquet(path)
              .coalesce(1)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=$start2/hour=$i")
          } else {
            println(s"跳过 :  $path")
          }
          a = a + 1
          if (start1 == end_time) {
            breaks.break()
          }
        }
      }
    }

    getBetweenDates("2023-04-13 09:00:00", "2023-04-20 23:00:00")

  }
}
