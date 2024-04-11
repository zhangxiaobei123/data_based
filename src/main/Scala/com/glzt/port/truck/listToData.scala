package com.glzt.port.truck

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}

object listToData {
  def main(args: Array[String]): Unit = {


    // 创建 SparkSession 对象
    val spark = SparkSession
      .builder()
      .appName("my local spark")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    // 时间元素列表
    val timeList = List(23, 4, 12, 10, 53, 39)

    //
    //    // 定义 DataFrame 模式
        val schema = new StructType()
        schema.add("year", IntegerType)
        schema.add("month", IntegerType)
        schema.add("day", IntegerType)
        schema.add("hour", IntegerType)
        schema.add("minute", IntegerType)
        schema.add("second", IntegerType)

    // 创建 DataFrame
    val df = spark.createDataFrame(Seq((timeList), (List(21, 6, 30, 8, 45, 12)))).toDF("time_list")

    // 定义 UDF
    val convertToDate = udf((timeList1: Seq[Int]) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = new Date(timeList1(0) - 1900, timeList1(1) - 1, timeList1(2), timeList1(3), timeList1(4), timeList(5))
      sdf.format(date)
    })

    // 使用 UDF 转换列
    val result = df.withColumn("time_string", convertToDate(col(s"time_list")))

    // 显示结果
    result.show()

  }
}
