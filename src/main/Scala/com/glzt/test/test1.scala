package com.glzt.test

import org.apache.spark.sql.SparkSession

object test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("")
      .enableHiveSupport()
      .getOrCreate()

    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "zhangyaobin")
      .option("password", "cFrb$BBc8g6^mQBl")
      .option("dbtable", "obd_realtime_data_bak")

    val dataFrame = reader.load()

    dataFrame.selectExpr("device_id", "scr_6413", "enter_air_6415", "ef_6411", "published_at")



  }
}
