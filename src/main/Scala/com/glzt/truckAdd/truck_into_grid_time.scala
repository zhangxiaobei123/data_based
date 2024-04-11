package com.glzt.truckAdd


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, lag, lead, monotonically_increasing_id}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * @Author:Tancongjian
 * @Date:Created in 10:26 2023/3/29
 *
 */
object truck_into_grid_time {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("运渣车停住点进出网格")
      .master("local[*]")
      .getOrCreate()


//    spark.close()
    val grid = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .option("dbtable", "od_fixed_grid")
      .load()
      .selectExpr("grid_id", "bottom_left_lon", "bottom_left_lat","top_right_lon","top_right_lat")
      .persist()



    val tableName = s"(SELECT sim_card,lat,lon,published_at,published_at_raw FROM truck_stay_point where published_at>='2023-03-28 00:00:00' and published_at<'2023-03-29 00:00:00') t"

    val reder_data = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .option("dbtable", tableName)
      .load()
      .dropDuplicates("sim_card","lat","lon","published_at")
      .selectExpr("sim_card","lat","lon","published_at_raw")
      .persist()

    val data = reder_data.join(broadcast(grid), col("lat").>(grid.col("bottom_left_lat"))
        && col("lon").>(grid.col("bottom_left_lon"))
        && col("lat").<=(grid.col("top_right_lat"))
        && col("lon").<=(grid.col("top_right_lon")))
      .selectExpr("sim_card","published_at_raw","grid_id")
      data.show()

    val data_by = data
      .withColumn("grid_id_lag",lag(col("grid_id"),1,0)
      .over(Window.partitionBy("sim_card").orderBy("published_at_raw")))
      .withColumn("grid_id_lead",lead(col("grid_id"),1,0)
      .over(Window.partitionBy("sim_card").orderBy("published_at_raw")))
      .filter(col("grid_id_lag").=!=(col("grid_id")) || col("grid_id_lead").=!=(col("grid_id")))



    val data_in = data_by
      .filter(col("grid_id"). =!= (col("grid_id_lag")))
      .withColumnRenamed("published_at_raw","published_at_in")
      .withColumn("id",monotonically_increasing_id())
      .drop("grid_id_lag","grid_id_lead")

    val data_out = data_by
      .withColumnRenamed("published_at_raw","published_at_out")
      .filter(col("grid_id"). =!= (col("grid_id_lead")))
      .withColumn("id",monotonically_increasing_id())
      .drop("grid_id_lag","grid_id_lead")


   val last_data =  data_in.join(data_out,Seq("sim_card","grid_id","id"),"inner").drop("id")
//     .filter("sim_card = '川ADE739'")
     .orderBy("published_at_in")



    last_data.show()
    reder_data.unpersist()
    spark.stop()

  }
}
