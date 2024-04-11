import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, substring}

/***
 * 导出DM8数据到 Hive
 * DM8 导入 Hive 全量数据
 * spark-shell --master yarn --deploy-mode client --conf spark.sql.shuffle.partitions=10 --executor-memory 2g --executor-cores 2 --num-executors 2 --jars hdfs://GlztCluster/dolphinscheduler/root/resources/util_jar
 */
object dm {
  def main(args: Array[String]): Unit = {

/*
        val sparkConf=new SparkConf()
        val spark = SparkSession.builder().master("local[*]").config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate()
    /*    def dm2hive(source_db_tb: String, hive_table: String) = {

      val query =
        s"""
           |select * from $source_db_tb
           |""".stripMargin

      val url = "jdbc:dm://192.1.19.130:5236/CTWRY_DATA?autoReconnect=true"
      val driver = "dm.jdbc.driver.DmDriver"
      val data = spark.read.format("jdbc")
        .option("driver", s"${driver}")
        .option("url", s"${url}")
        .option("dbtable", s"($query) t")
        .option("user", "glzt_dev")
        .option("password", "mV3Ab5dqj")
        .load()
      data.write.mode(SaveMode.Overwrite).insertInto(hive_table)
    }
    dm2hive("CTWRY_DATA.T_AUTO_BASE_INFO_WATER_REFER","ods_ctwry_data.ods_t_auto_base_info_water_refer")*/

    def dmhive(dm_table: String, hive_table: String, time: String, start_time: String, end_time: String) {
      val query = s"(SELECT * FROM $dm_table where $time>='$start_time' and $time<'$end_time') t"
      val db_tb = dm_table.split("\\.")
      val db_name = db_tb(0)
      val dm_df = spark.read.format("jdbc")
        .option("driver", "dm.jdbc.driver.DmDriver")
        .option("url", s"jdbc:dm://192.1.19.130:5236/$db_name?autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true")
        .option("dbtable", query)
        .option("user", "glzt_dev")
        .option("password", "mV3Ab5dqj")
        .load()
      dm_df
        .withColumn("publish_day", substring(col(time), 0, 10))
        .repartition(1)
        .write
        .mode(SaveMode.Append)
        .insertInto(hive_table)
    }
  dmhive("CTWRY_DATA.T_SPECIAL_INDUSTRY_SUPERVISE_SOURCE","ods_ctwry_data.ods_t_special_industry_supervise","CREATED_TIME","2020-12-30","2021-01-01")

*/




  }
}
