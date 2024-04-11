package com.glzt.obd


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._
import java.sql.DriverManager
/** *
 * 根据
 * select
 *    *
 * from
 *    obd_realtime_data_bak ordb2
 * where
 *    device_id in
 * (
 * select
 *    device_id
 * from
 *      obd_realtime_data_bak ordb
 * where
 *      dynamic_data->'$."6413"' != 0
 * and
 *      dynamic_data->'$."6415"' !=0
 * and
 *      dynamic_data->'$."6411"' !=0
 * group by device_id
 * having count(device_id)>10000
 * )
 * having dynamic_data->'$."6413"' != 0 and dynamic_data->'$."6415"' !=0 and dynamic_data->'$."6411"' !=0
 * 这个条件，把HIVE里面的OBD数据处理以后写入到算法库
 *
 * 先过滤 在统计大于一万的
 */

object Data_1_obd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("obd数据插入 先过滤 在统计大于一万 ")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    val last = spark
      .sql("select * from ods_traffic.ods_obd_realtime_data")

    val schema = new StructType()
      .add("6413", DoubleType)
      .add("6415", DoubleType)
      .add("6411", DoubleType)

    val datas = last
      .select("device_id", "dynamic_data","error_data","published_at")
      .withColumn("dynamic_data_json", from_json(col("dynamic_data"), schema))
      .filter(col("dynamic_data_json.6413") =!= 0  and  col("dynamic_data_json.6415") =!= 0 and col("dynamic_data_json.6411") =!= 0)
      .drop("dynamic_data_json")
      .na.drop()
      .selectExpr("device_id", "dynamic_data","error_data","published_at")
       // 创建临时表
      .createOrReplaceTempView("realtime_data")
//    过滤临时表 数据 大于一万的
    val last1 = spark
      .sql("select * from realtime_data where device_id in (select device_id FROM realtime_data where group by device_id having count(device_id) > 10000)")
//      .show()

//       批量提交 写入mysql
    last1.repartition(1)
          .rdd
          .foreachPartition(dataList => {
            val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true"
            val username = "zhangyaobin"
            val password = "cFrb$BBc8g6^mQBl"
            Class.forName("com.mysql.jdbc.Driver")
            val conn = DriverManager.getConnection(url, username, password)
            val sql = "replace into obd_realtime_data_bak(device_id,dynamic_data,error_data,published_at) values(?,?,?,?)"
            val ps = conn.prepareStatement(sql)
            var batchIndex = 0
            dataList.foreach(data => {
              ps.setInt(1, data.getInt(0))
              ps.setString(2, data.getString(1))
              ps.setString(3, data.getString(2))
              ps.setString(4, data.getString(3))
              // 加入批次
              ps.addBatch()
              batchIndex += 1
              // 控制提交的数量,
              // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！！！
              if (batchIndex % 20000 == 0 && batchIndex != 0) {
                ps.executeBatch()
                ps.clearBatch()
              }
            })
            ps.executeBatch()
            conn.setAutoCommit(false)
            conn.commit()
            conn.close()
          })
//    3,701,345
  }
}


