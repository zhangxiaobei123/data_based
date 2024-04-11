package com.glzt.ods

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.Try
import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}


/** *
 * 小文件合并 写入临时表
 */


object Truck_merge {
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

//    将合并后的文件从临时表写回目标表中
    def getBetweenDates(start_time: String, end_time: String): Unit = {
      var a = 0
      val breaks = new Breaks
      breaks.breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)
          val start2 = start1.substring(0, 10)
          val i = start1.substring(11, 13)
          val path = s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_slag_truck_interface/publish_date=$start2/hour=$i"
          if (Try(spark.read.parquet(path)).isSuccess) {
            println(path)
            val df = spark.read.parquet(path)
              .coalesce(1)
              .write
              .mode(SaveMode.Append)
              .parquet(s"hdfs://hacluster/user/hive/warehouse/ods_traffic.db/ods_slag_truck_interface_test1/publish_date=$start2/hour=$i")
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





    getBetweenDates("2023-04-13 00:00:00", "2023-04-20 23:00:00")
  }
}
