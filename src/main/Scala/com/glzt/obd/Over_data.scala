package com.glzt.obd

import com.glzt.truckAdd.JDBCUtils

import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.control.Breaks.{break, breakable}

object Over_data {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession
       .Builder()
      .getOrCreate()

    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }

//   5.6 12点
    def truck_road() {
      val tableName = s"(SELECT grid_id,station_code,station_name,data,published_at FROM station_portrait_feature where published_at >= '2022-06-01 00:00:00' AND published_at < '2022-12-31 23:00:00') t"
      val reader = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.108.37:3306/maps_calculate?Unicode=true&characterEncoding=utf-8&useSSL=false")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        //        .option("user", "glzt-pro-bigdata")
        //        .option("password", "Uhh4QxUwiMsQ4mK4")
        .option("user", "zhangyaobin")
        .option("password", "cFrb$BBc8g6^mQBl")
        .option("dbtable", tableName)
        .load()
      reader.select("grid_id", "station_code", "station_name", "data", "published_at")
        .coalesce(1)
        .foreachPartition((partition: Iterator[Row]) => {
          var connect: Connection = null
          var pstmt: PreparedStatement = null

//          try {

            connect = JDBCUtils.getConnection
            // 禁用自动提交
            connect.setAutoCommit(false)
            val sql = "REPLACE INTO `alpha-center`.`station_portrait_feature`(grid_id,station_code,station_name,data,published_at) VALUES(?, ?, ?, ?, ?)"
            pstmt = connect.prepareStatement(sql)
            var batchIndex = 0
            partition.foreach(x => {
              pstmt.setString(1, x.getInt(0).toString)
              pstmt.setString(2, x.getString(1))
              pstmt.setString(3, x.getString(2))
              pstmt.setString(4, x.getString(3))
              pstmt.setString(5, x.getTimestamp(4)toString)
              // 加入批次
              pstmt.addBatch()
              batchIndex += 1
              // 控制提交的数量,
              // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！！！
              if (batchIndex % 20000 == 0 && batchIndex != 0) {
                pstmt.executeBatch()
                pstmt.clearBatch()
              }
            })
            // 提交批次
            pstmt.executeBatch()
            connect.commit()

//          } catch {
//            case e: Exception =>
//              e.printStackTrace()
//          } finally {

            JDBCUtils.closeConnection(connect, pstmt)
//          }
        })
    }

//    val data = readerMetaData("2023-05-01 01:00:00", "2023-05-06 12:00:00")
//    truck_road(data)




  }
}
