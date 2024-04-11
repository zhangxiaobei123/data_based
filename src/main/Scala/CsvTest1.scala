

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.rdd.RDD

/***
 * 读取csv文件写入hive 解析json 解决json错位问题
 * start_time = 开始时间
 * end_time = 结束时间
 * next_date = 开始时间 +1天的时间
 */


object CsvTest1 {
  def main(args: Array[String]): Unit = {

    var a = 0
    var start_time = ""
    var end_time = "20230314"
    var next_date: String = ""
    val spark = SparkSession
      .builder()
      .appName("csv_hive")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    def getDate(start_time: String, a: Int): String = {
      val df = new SimpleDateFormat("yyyyMMdd")
      val date = df.parse(start_time)
      val cal: Calendar = Calendar.getInstance()
      cal.setTime(date)
      cal.add(Calendar.DATE, a)
      df.format(cal.getTime())
    }

    val sc = spark.sparkContext

    while (start_time < end_time) {
      start_time = getDate("", a)
      next_date = getDate(start_time, 1)

      println(start_time)
      println(next_date)
//      读取csv文件转换为text
      val csvRdd: RDD[String] = sc.textFile(s"file:///srv/BigData/data1/MOBILE_STATION_REALTIME_AQ_RAW/SYSTEM_SYSDBA_MOBILE_STATION_REALTIME_AQ_RAW_${start_time}_${next_date}_20230331.csv")
      //将CSV文件的表头去掉
      //mapPartitionsWithIndex 也是按照分区进行的map操作，不过mapPartitionsWithIndex传入的参数多个一个分区的值
      val noheadRdd: RDD[String] = csvRdd.mapPartitionsWithIndex((i, v) => {
        if (i == 0)
          v.drop(1)
        else
          v
      })
      // 转换成map按','切分
      val files = noheadRdd.map(
        record => {
          val split = record.split(",")
          val length = split.length
          val id = split(0)
          val station_id = split(1)
          val station_code = split(2)
          val grid_id = split(3)
          val lon = split(4)
          val data = split(6)
          val published_at = split(length - 1)
          val coord = new StringBuilder(lon)
          val datas = new StringBuilder(data)
          coord.append("," + split(5))  // 把coord按都好继续合并
          var index = 7//   把datas按都好继续合并
          while (index < length - 2) {
            datas.append(",").append(split(index))
            index += 1
          }
          datas.append("," + split(length - 2))
          csvFile(id, station_id, station_code, grid_id, coord.toString(), datas.toString(), published_at)
          //        val info=id+"\t"+station_id+"\t"+station_code+"\t"+grid_id+"\t"+coord.toString()+"\t"+datas.toString()+"\t"+published_at
          //        info
        }
      )
      import spark.implicits._
       //  转成DF
      files.toDF()
        .coalesce(1)
        .createOrReplaceGlobalTempView("temp")
      val sql = "insert into table ods_air.ods_mobile_station_realtime_aq_raw partition(publish_date) select *,substring(from_unixtime(unix_timestamp(published_at)),1,10) publish_date from global_temp.temp"
      spark.sql(sql)
      a += 1
    }
    sc.stop()
    spark.close()
  }

  case class csvFile(id: String, station_id: String, station_code: String, grid_id: String, coord: String, data: String, published_at: String)

}


