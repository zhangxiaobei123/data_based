package com.glzt.test


import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.util.control.Breaks.{break, breakable}

object TimeTest {

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession
    .Builder()
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()


    def dealDateFormat(oldDateStr: String): String = {
      val df = new SimpleDateFormat("yyyy/MM/d HH:mm")
      val date = df.parse(oldDateStr)
      val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df2.format(date)
    }


     //   读取hdfs 中csv的文件
//    val times = spark.read.option("header", true).csv("hdfs://hacluster/dolphinscheduler/root/resources/data/time11.csv").collect().toList
//    for (time <- times) {
//      val time1 = dealDateFormat(time.get(0).toString)
//      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val date = df.parse(time1)
//      val cal = Calendar.getInstance()
//      cal.setTime(date)
//      val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
//      println(published_at)
//      val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
//      println(end)
//      val published_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
//      println(published_date)
//      cal.add(Calendar.HOUR, -2)
//      val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
//      println(start)
//    }


    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }

    def getHoursTime1(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:59:59")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }

    def getCurrHourTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, -int)
      val time = df.format(calendar.getTime)
      time
    }


    def getCurrHourTime1(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:59:59")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, -int)
      val time = df.format(calendar.getTime)
      time
    }

//  日期格式转换
    def getTransTime(timeStr: String): String = {
      val df = DateTimeFormatter.ofPattern("yyyy-M-d H:m:s");
      val date = LocalDateTime.parse(timeStr, df);
      val f2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      val transTime = f2.format(date);
      transTime
    }

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -1)
    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
    val published_data = published_at.substring(0, 10)
    //    cal.add(Calendar.HOUR, -2)
    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
//    val data = recordMetaData(published_at, end, start, published_data)

//    val cal = Calendar.getInstance()
//    cal.add(Calendar.HOUR, -1)
//    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
//    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
//    val published_data = published_at.substring(0, 10)
//    //    cal.add(Calendar.HOUR, -2)
//    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
//    val data = recordMetaData(published_at, end, start, published_data)
//
//    val cal = Calendar.getInstance()
//    cal.add(Calendar.HOUR, -2)
//    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    println(published_at)
//    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
    println(end)
//    val published_data = published_at.substring(0, 10)
    println(published_data)
//    //    cal.add(Calendar.HOUR, -2)
//    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    println(start)
//
//
//
//
//    for (i <- 1 to 3) {
//      var a = 0
//      val published_at1 = getCurrHourTime(published_at, i)
//      val published_date1 = published_at1.substring(0, 10)
//      println(published_date1)
//      println(published_at1)
//      val start1 = getCurrHourTime(start, i)
//      println(start1)
//      val end1 = getCurrHourTime1(end, i)
//      println(end1)
//      a = a + i
//    }


//    判断经纬度
//    def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Long = {
//      val EARTH_RADIUS = 6371393 //赤道半径(单位m)
//      val radLat1 = lat1 * Math.PI / 180.0 // 将经度维度除以180再乘以π，转换为弧度
//      val radLat2 = lat2 * Math.PI / 180.0
//      val a = radLat1 - radLat2
//      val radLon1 = lon1 * Math.PI / 180.0
//      val radLon2 = lon2 * Math.PI / 180.0
//      val b = radLon1 - radLon2
//      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
//      Math.round(s * EARTH_RADIUS * 10000) / 10000
//    }


    //            val published_at = "2023-04-19 18:00:00"
    //            val start = published_at
    //            val published_date= published_at.substring(0,10)
    //            val end = getHoursTime(published_at,1)
    //


    //              val cal = Calendar.getInstance()
    //              cal.add(Calendar.HOUR, -1)
    //              val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    //              println("开始时间:" + published_at)


    //              val end = published_at
    //              println(end)


    //              val start =getHoursTime(published_at,1)
    //              println(start)


    //              val published_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //             println(published_date)


    //       获取当前时间的前两个小时

    //    val cal = Calendar.getInstance()
    //    cal.add(Calendar.HOUR, -1)
    //    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    //    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
    //    val published_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //    cal.add(Calendar.HOUR, -2)
    //    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)


    //    val cal = Calendar.getInstance()
    //    cal.add(Calendar.HOUR, -1)
    //    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    //    println(published_at)
    //    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
    //    println(end)
    //    val published_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //    println(published_date)
    //    cal.add(Calendar.HOUR, -2)
    //    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    //    println(start)

//        getBetweenDates("2023-02-01 00:00:00" , "2023-03-01 00:00:00")

//    getBetweenDates("2023-02-01 00:00:00", "2023-02-05 00:00:00","2023-02-01 00:59:59")

//    def getBetweenDates(start_time: String, end_time: String, end: String): Unit = {
//      //解析
//      var a = 0
//      breakable {
//        while (true) {
//          val start1 = getHoursTime(start_time, a)
//          val published_at1 = start1
//          val end1 = getHoursTime1(end, a)
//          val published_date1 = published_at1.substring(0, 10)
//
//
//          println(published_at1)
//          println(published_date1)
//          println(start1)
//          println(end1)
//
//          //          val published_at = "2023-01-01 00:00:00"
//          //          val end = "2023-01-01 00:59:59"
//          //          val start = "2022-12-31 22:00:00"
//          //          val published_date = "2023-01-01"
//          //          val publish_date = "2022-12-31"
//          a = a + 1
//          if (start1 == end_time) {
//            break
//          }
//        }
//      }
//    }


    def getBetweenDates(start_time: String, end_time: String, end: String): Unit = {
      //解析
      var a = 0
      breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)
          println(start1)
          val published_at1 = start1
          println(published_at1)
          val end1 = getHoursTime1(end, a)
          println(end1)
          val published_date1 = published_at1.substring(0, 10)
          println(published_date1)
          val hour = start1.substring(11, 13)
          println(hour)
//          val data = recordMetaData(published_at1, end1, start1, published_date1)
//
//          truck_road(data)

          a = a + 1
          if (start1 == end_time) {
            break
          }
        }
      }
    }
//
//
//        getBetweenDates("2023-01-01 00:00:00", "2023-01-01 23:00:00","2023-01-01 00:59:59")

  }
}

