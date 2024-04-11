
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, json_tuple}
import org.apache.spark.sql.types.DoubleType

import java.sql.DriverManager


/**
 * 把hiveobd的数据插入到center表中
 *
 */
object obd_realtime_data {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

//        val list = List("02","07","08","09","10","11","12","13","14","15","21","22","23","24","25","26","28","29","30")
//    val list = List("01","02","23","24","28","29","30")
//    val list = List(18, 21, 22, 23)
    val list = List("01", "02", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22")
//    val list= List("01", "02", "03", "04", "05", "06", "07","08","09","10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22","23","24","25","26","27","28","29","30","31")
    for (i <- list) {


      val result_data = spark.read
        .parquet(s"obs://bigdata-glzt/datahouse/traffic/ods/ods_traffic/ods_obd_realtime_data/publish_date=2022-06-$i")
        .withColumn("lon",json_tuple(col("dynamic_data"),"6423"))
        .withColumn("lat",json_tuple(col("dynamic_data"),"6422"))
        .withColumn("oil",json_tuple(col("dynamic_data"),"6420"))
        .withColumn("lon",col("lon").cast(DoubleType))
        .withColumn("lat",col("lat").cast(DoubleType))
        .withColumn("oil",col("oil").cast(DoubleType))
        .na.drop()
        .selectExpr("device_id","lon","lat","oil","published_at")
//
//println(result_data.na.drop().count())  // 去空值
//println(result_data.count())
//result_data.show()




      // 插入数据到
      result_data.repartition(1)
        .rdd
        .foreachPartition(dataList => {
          val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true"
          val username = "zhangyaobin"
          val password = "cFrb$BBc8g6^mQBl"
          Class.forName("com.mysql.jdbc.Driver")
          val conn = DriverManager.getConnection(url, username, password)
          val sql = "replace into obd_realtime_data(device_id,lon,lat,oil,published_at) values(?,?,?,?,?)"
          val ps = conn.prepareStatement(sql)
          var batchIndex = 0
          dataList.foreach(data => {
            ps.setInt(1, data.getInt(0))
            ps.setDouble(2, data.getDouble(1))
            ps.setDouble(3, data.getDouble(2))
            ps.setDouble(4, data.getDouble(3))
            ps.setString(5, data.getString(4))
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
    }
  }

}