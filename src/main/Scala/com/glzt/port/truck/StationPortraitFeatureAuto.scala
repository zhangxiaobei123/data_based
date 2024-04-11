package com.glzt.port.truck

import com.glzt.truckAdd.JDBCUtils


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.util.control.Breaks.{break, breakable}
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}


object StationPortraitFeatureAuto {

  case class station_class(var grid_id: String, var station_name: String, var station_code: String, var wind_speed: Double)

  def main(args: Array[String]): Unit = {

    val spark = new SparkSession
      .Builder()
      .enableHiveSupport()
      .getOrCreate()


    def getHoursTime1(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:59:59")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
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

    def getCurrHourTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, -int)
      val time = df.format(calendar.getTime)
      time
    }

    def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Long = {
      val EARTH_RADIUS = 6371393 //赤道半径(单位m)
      val radLat1 = lat1 * Math.PI / 180.0
      val radLat2 = lat2 * Math.PI / 180.0
      val a = radLat1 - radLat2
      val radLon1 = lon1 * Math.PI / 180.0
      val radLon2 = lon2 * Math.PI / 180.0
      val b = radLon1 - radLon2
      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
      Math.round(s * EARTH_RADIUS * 10000) / 10000
    }

    def getWind(lon: Double, lat: Double, wind_DF: DataFrame): BigDecimal = {
      val sparkWind = wind_DF.sparkSession
      sparkWind.udf.register("dis2", dis2 _)
      wind_DF.createOrReplaceTempView("wind_data")
      sparkWind.sql(
        s"""
           |select
           |   station_name,
           |   wind_speed,
           |   1 / dis2(lon, lat, ${lon}, ${lat}) as dis2_1
           |from wind_data
           |""".stripMargin)
        .createOrReplaceTempView("tmp")

      val wind = sparkWind.sql(
        s"""
           |select
           |   cast(sum(wind_speed * dis2_1)/sum(dis2_1) as decimal(8,2)) as wind_speed
           |from tmp
           |""".stripMargin)
      var wind_speed: BigDecimal = 0
      wind.head(1).foreach(x => {
        if (x.getDecimal(0) == null) {
          wind_speed = 0
        } else {
          wind_speed = x.getDecimal(0)
        }
      })
      wind_speed
    }

    def dis2(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Long = {
      val EARTH_RADIUS = 6371393
      val radLat1 = lat1 * Math.PI / 180.0
      val radLat2 = lat2 * Math.PI / 180.0
      val a = radLat1 - radLat2
      val radLon1 = lon1 * Math.PI / 180.0
      val radLon2 = lon2 * Math.PI / 180.0
      val b = radLon1 - radLon2
      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
      val dis = Math.round(s * EARTH_RADIUS * 10000) / 10000
      dis * dis
    }

    def getDF(query: String, spark: SparkSession): DataFrame = {
      spark.read.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&allowPublicKeyRetrieval=true")
        .option("dbtable", query)
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4")
        //      .option("user", "tancongjian")
        //      .option("password", "mK81VrWmFzUUrrQd")
        .load()
    }

    def getHoursTime(string: String, int: Int): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(df.parse(string))
      calendar.add(Calendar.HOUR, +int)
      val time = df.format(calendar.getTime)
      time
    }

    def getDF1(query: String, spark: SparkSession): DataFrame = {
      spark.read.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://192.168.108.37:3306/maps_calculate?useSSL=false&allowPublicKeyRetrieval=true")
        .option("dbtable", query)
        .option("user", "zhangyaobin")
        .option("password", "cFrb$BBc8g6^mQBl")
        .load()
    }



    def recordMetaData(published_at: String, end: String, start: String, published_date: String,hour:String): DataFrame = {

                val published_at = "2022-12-17 14:00:00"
                val end = "2022-12-17 23:59:59"
                val start = "2022-12-17 00:00:00"
                val published_date = "2022-12-17"
                val hour = "14"

      import spark.implicits._
      val station_info: String =
        s"""
           |(select
           |  json_extract(coord, '$$.lon') as lon,
           |  json_extract(coord, '$$.lat') as lat,
           |  grid_id,
           |  station_code,
           |  station_name
           |from aq_fixed_station
           |where grid_id is not null
           |and station_level in('nation', 'province','city')
           |) t
           |""".stripMargin

            val slag_data_DF = spark.sql(
              s"""
                 |SELECT
                 |  plate_num,
                 |  sim_card,
                 |  up_time,
                 |  lon,
                 |  lat
                 |FROM ods_traffic.ods_slag_truck_interface
                 |WHERE  publish_date ='${published_date}' and hour = '${hour}'
                 |AND speed > 0
                 |""".stripMargin).dropDuplicates("plate_num","up_time")

      slag_data_DF.show()
      slag_data_DF.createOrReplaceTempView("view_slag_data")
//      val slag_data: String =
//      s"""
//         |(SELECT
//         |  plate_num,
//         |  sim_card,
//         |  json_extract(coord, '$$.lon') as lon,
//         |  json_extract(coord, '$$.lat') as lat
//         |FROM slag_truck_realtime
//         |WHERE published_at between '${start}' and '${end}'
//         |AND speed > 0
//         |) t
//         |""".stripMargin
//

      val index_data: String =
        s"""
           |(SELECT
           |  json_extract(grid_center_coord, '$$.lon') as lon,
           |  json_extract(grid_center_coord, '$$.lat') as lat,
           |  json_extract(data, '$$.index') as index_num
           |FROM grid_hour_traffic_cd
           |WHERE published_at = '${published_at}'
           |) t
           |""".stripMargin

      val station_info_DF = getDF(station_info, spark)
      station_info_DF.createOrReplaceTempView("view_station_info")
      //      station_info_DF.show()

//      val slag_data_DF = getDF(slag_data, spark)

      val index_data_DF = getDF(index_data, spark)
      index_data_DF.createOrReplaceTempView("view_index_data")

      // todo 去除静止的运渣车数量，需先去重 -- 求和
      // todo 去除静止的运渣车轨迹点 -- 求和
      spark.udf.register("distance", distance _)
      val res1 = spark.sql(
        """
          |select
          |   b.grid_id,
          |   b.station_code,
          |   b.station_name,
          |	  count(DISTINCT a.plate_num, a.sim_card) as truck_num,
          |	  count(*) as truck_trajectory
          |from view_slag_data a join view_station_info b
          |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
          |group by b.grid_id, b.station_code, b.station_name
          |""".stripMargin)
      res1.createOrReplaceTempView("view_res1")
      // todo 拥堵指数 -- 求平均
      val res3 = spark.sql(
        """
          |select
          |   b.grid_id,
          |   b.station_code,
          |   b.station_name,
          |	  avg(index_num) as traffic_index
          |from view_index_data a join view_station_info b
          |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
          |group by b.grid_id, b.station_code, b.station_name
          |""".stripMargin)
      res3.createOrReplaceTempView("view_res3")
      //          res3.show(5)

      // todo  湿度、太阳辐射、降雨量 -- 求平均
      val weather_query: String =
        s"""
           |(SELECT
           |  json_extract(grid_center_coord, '$$.lon') as lon,
           |  json_extract(grid_center_coord, '$$.lat') as lat,
           |  cast(json_extract(data, '$$.rainfall') as double) as rainfall,
           |  cast(json_extract(data, '$$.humidity') as double) as humidity,
           |  cast(json_extract(data, '$$.solar_radiation') as double) as solar_radiation
           |FROM grid_hour_weather
           |WHERE published_at = '${published_at}'
           |) t
           |""".stripMargin

      val weather_DF = getDF(weather_query, spark)
      //      气象数据
      //      weather_DF.show()

      weather_DF.createOrReplaceTempView("view_weather")

      val res4 = spark.sql(
        """
          |select
          |   b.grid_id,
          |   b.station_code,
          |   b.station_name,
          |   cast(avg(a.rainfall) as decimal(8,2)) as rainfall,
          |   cast(avg(a.humidity) as decimal(8,2)) as humidity,
          |   cast(avg(a.solar_radiation) as decimal(8,2)) as solar_radiation
          |from view_weather a join view_station_info b
          |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
          |group by b.grid_id, b.station_code, b.station_name
          |""".stripMargin)
      res4.createOrReplaceTempView("view_res4")

      //      res4.show()
      // todo  道路扬尘指数 -- 求 sum
      val dust_query: String =
      s"""
         |(SELECT
         |  grid_id,
         |  slag_truck_coords
         |FROM road_dust_grid_cd
         |WHERE published_at = '${published_at}'
         |) t
         |""".stripMargin

      val dust_DF = getDF(dust_query, spark)
      //      dust_DF.show()
      val dust_DF2 = dust_DF.flatMap(line => {
        val grid_id = line.getInt(0)
        line.getString(1).replace(" ", "").split("],\\[")
          .map(x => {
            val contents = x.replace("[", "").replace("]", "")
              .split(",")
            (grid_id, contents(0))
          })
      }).toDF("grid_id", "index")
      dust_DF2.createOrReplaceTempView("view_dust")
      //        dust_DF2.show()
      val query_4900: String =
        s"""
           |(SELECT
           |  id,
           |  json_extract(center_coord, '$$.lon') as lon,
           |  json_extract(center_coord, '$$.lat') as lat
           |FROM grid_cd_one_ride_one
           |) t
           |""".stripMargin
      val DF_4900 = getDF(query_4900, spark)
      DF_4900.createOrReplaceTempView("view_4900")
      //      DF_4900.show()
      val res5 = spark.sql(
        """
          |select
          |   c.grid_id,
          |   c.station_code,
          |   c.station_name,
          |   cast(sum(t.dust_index) as int) as dust_index
          |from (select
          |   b.id,
          |   b.lon,
          |   b.lat,
          |   sum(round(a.index)) as dust_index
          |from view_dust a join view_4900 b
          |on a.grid_id = b.id
          |group by b.id, b.lon, b.lat) t join view_station_info c
          |on distance(t.lon, t.lat, c.lon, c.lat) < 5000
          |group by c.grid_id, c.station_code, c.station_name
          |""".stripMargin)
      res5.createOrReplaceTempView("view_res5")
      //res5.show()

      //todo 9 风速优化：站点到12个气象站的距离加权值
      val wind_query: String =
        s"""
           |(SELECT
           |  station_name,
           |  json_extract(station_coord, '$$.lon') as lon,
           |  json_extract(station_coord, '$$.lat') as lat,
           |  json_extract(data, '$$.wind_speed') as wind_speed
           |FROM station_hour_weather
           |WHERE published_at = '${published_at}'
           |AND station_name in ("成都", "金牛", "青羊", "武侯", "锦江", "成华", "新都", "青白江", "郫都", "温江", "双流", "龙泉驿")
           |) t
           |""".stripMargin
      val wind_DF = getDF(wind_query, spark)
      //      wind_DF.show()
      val res6 = station_info_DF.collect().toList
        .map(x => {
          val lon = x(0).toString
          val lat = x(1).toString
          val grid_id = x(2).toString
          val station_code = x(3).toString
          val station_name = x(4).toString
          var wind_speed: Double = 0
          if (!wind_DF.rdd.isEmpty()) {
            wind_speed = getWind(lon.toDouble, lat.toDouble, wind_DF).toDouble
          }
          station_class(grid_id, station_name, station_code, wind_speed)
        }).toDF()
      res6.createOrReplaceTempView("view_res6")

      //res6.show()
      //todo 10 扩散条件 -- 直接读表取值
      val diffusion_conditions_query: String =
      s"""
         |(select
         |  SUBSTRING(diffusion_conditions, 1,1) as diffusion_conditions
         |from weather_forecast_spider
         |where published_at = (select MAX(published_at) from weather_forecast_spider where forecast_date = '${published_date}')
         |and forecast_date = '${published_date}'
         |) t
         |""".stripMargin
      val res7 = getDF(diffusion_conditions_query, spark)
      //        res7.show()
      var diffusion_conditions = "0"
      res7.head(1).foreach(x => {
        if (x.get(0) == null) {
          diffusion_conditions = "0"
        } else {
          diffusion_conditions = x.getString(0)
        }
      })

      //todo 11 土方量 -- 直接读表取值 求和
      val earth_query: String =
        s"""
           |(select
           |	a.grid_id,
           |	a.truck_num,
           |  json_extract(b.center_coord, '$$.lon') as lon,
           |  json_extract(b.center_coord, '$$.lat') as lat,
           |  a.published_at
           |from truck_stay_point_earth_volume a join grid_cd_one_ride_one b
           |on a.grid_id = b.id AND a.published_at = '${published_at}'
           |) t
           |""".stripMargin
      val earth_DF = getDF(earth_query, spark)
      earth_DF.createOrReplaceTempView("view_earth")
      //      earth_DF.show()
      val res8 = spark.sql(
        """
          |select
          |   b.grid_id,
          |   b.station_code,
          |   b.station_name,
          |	  sum(a.truck_num) as earth_num
          |from view_earth a join view_station_info b
          |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
          |group by b.grid_id, b.station_code, b.station_name
          |""".stripMargin)
      res8.createOrReplaceTempView("view_res8")
      //res8.show()
      //todo 12 交通流量 -- 联合实验室结果
      val flow_query: String =
      s"""
         |(select
         |c.id,
         |c.roadname,
         |c.section_gcj02_json,
         |t.flow,
         |t.published_at
         |from road_division_sanhuan c join
         |(select
         |	b.road_congested_section_id,
         |  if(json_extract(a.`data`, '$$.large') is null, 0, json_extract(a.`data`, '$$.large')) +
         |  if(json_extract(a.`data`, '$$.middle') is null, 0, json_extract(a.`data`, '$$.middle')) +
         |  if(json_extract(a.`data`, '$$.small') is null, 0, json_extract(a.`data`, '$$.small')) +
         |  if(json_extract(a.`data`, '$$.unknown') is null, 0, json_extract(a.`data`, '$$.unknown')) as flow,
         |	a.published_at
         |from road_traffic_hour_flow a join road_congested_section_feature b
         |on a.edge_id = b.edge_id and a.driving_direction = b.driving_direction and a.published_at = '${published_at}') t
         |on c.id = t.road_congested_section_id
         |) t
         |""".stripMargin
      val flow_df = getDF(flow_query, spark)
      //flow_df.show()
      val flow_df2 = flow_df.flatMap(line => {
        val id = line.getInt(0)
        val roadname = line.getString(1)
        val flow = line.getDouble(3)
        val published_at = line.getTimestamp(4)
        line.getString(2).replace(" ", "").split("],\\[")
          .map(x => {
            val contents = x.replace("[", "").replace("]", "")
              .split(",")
            (id, roadname, contents(0), contents(1), flow, published_at)
          })
      }).toDF("id", "roadname", "lon", "lat", "flow", "published_at")

      flow_df2.createOrReplaceTempView("view_flow_df2")
      //      flow_df2.show()
      val flow_df3 = spark.sql(
        """
          |select
          |   t.id,
          |   t.roadname,
          |   c.grid_id,
          |   c.station_code,
          |   c.station_name,
          |   max(t.flow) as flow
          |from view_flow_df2 t join view_station_info c
          |on distance(t.lon, t.lat, c.lon, c.lat) < 5000
          |group by t.id, t.roadname, c.grid_id, c.station_code, c.station_name
          |""".stripMargin)
      flow_df3.createOrReplaceTempView("view_flow_df3")
      //      flow_df3.show()
      val flow_df4 = spark.sql(
        """
          |select
          |   grid_id,
          |   station_code,
          |   station_name,
          |   cast(sum(flow) as int) as traffic_flow
          |from (select
          |     roadname, grid_id, station_code, station_name,
          |     avg(flow) as flow
          |     from view_flow_df3
          |     group by roadname, grid_id, station_code, station_name)
          |group by grid_id, station_code, station_name
          |""".stripMargin)
      flow_df4.createOrReplaceTempView("view_res2")
      //      flow_df4.show()

      spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  b.traffic_flow
           |from view_res1 a full join view_res2 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin).createOrReplaceTempView("view_12")

      spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  a.traffic_flow,
           |  cast(b.traffic_index as decimal(10,6)) as traffic_index
           |from view_12 a full join view_res3 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin).createOrReplaceTempView("view_123")

      spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  a.traffic_flow,
           |  a.traffic_index,
           |  b.rainfall,
           |  b.humidity,
           |  b.solar_radiation
           |from view_123 a full join view_res4 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin).createOrReplaceTempView("view_1234")
      spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  a.traffic_flow,
           |  a.traffic_index,
           |  a.rainfall,
           |  a.humidity,
           |  a.solar_radiation,
           |  b.dust_index
           |from view_1234 a full join view_res5 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin).createOrReplaceTempView("view_12345")
      spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  a.traffic_flow,
           |  a.traffic_index,
           |  a.rainfall,
           |  a.humidity,
           |  a.solar_radiation,
           |  a.dust_index,
           |  b.wind_speed,
           |  cast('${diffusion_conditions}' as Int) as diffusion_conditions,
           |  '${published_at}' as published_at
           |from view_12345 a full join view_res6 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin).createOrReplaceTempView("view_123456")
      val res = spark.sql(
        s"""
           |select
           |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
           |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
           |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
           |  a.truck_num,
           |  a.truck_trajectory,
           |  a.traffic_flow,
           |  a.traffic_index,
           |  a.rainfall,
           |  a.humidity,
           |  a.solar_radiation,
           |  a.dust_index,
           |  a.wind_speed,
           |  a.diffusion_conditions,
           |  a.published_at,
           |  b.earth_num
           |from view_123456 a full join view_res8 b
           |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
           |""".stripMargin)
      val resData = res.where(col("grid_id").isNotNull)
        .withColumn("data",
          to_json(struct($"truck_num", $"truck_trajectory", $"traffic_flow",
            $"traffic_index", $"rainfall", $"humidity", $"solar_radiation", $"dust_index", $"wind_speed",
            $"diffusion_conditions", $"earth_num"),
            Map("ignoreNullFields" -> "true")))
        .select("grid_id", "station_code", "station_name", "data", "published_at")
      resData
    }

    def readerMetaData(start: String): DataFrame = {

      val tableName = s"(SELECT grid_id,station_code,station_name,data,published_at FROM station_portrait_feature where published_at='$start') t"
      val reader = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4")
//                .option("user", "zhangyaobin")
//                .option("password", "cFrb$BBc8g6^mQBl")
        .option("dbtable", tableName)
        .load()
      reader
    }

    //    val mergeJson = udf((json1: String, json2: String) => {
    //      val JObject(fields1) = parse(json1)
    //      val JObject(fields2) = parse(json2)
    //      val mergedFields = fields1 ++ fields2
    //      compact(render(JObject(mergedFields)))
    //    })
    //    spark.udf.register("mergeJson", mergeJson)


    def truck_road(dataFrame1: DataFrame) {

      //          dataFrame1.select("grid_id", "station_code", "station_name", "data", "published_at")
      //      .createOrReplaceTempView("df1")
      //    val dataFrame2: DataFrame = recordMetaData(published_at, end, start, published_date, start)
      //        dataFrame2.select("grid_id", "station_code", "station_name", "data", "published_at")
      //      .createOrReplaceTempView("df2")

      //    d1.show()
      //    val d2 = dataFrame2.select("grid_id", "station_code", "station_name", "data", "published_at")
      /*   val d4 = spark.sql(
           """
             |select
             |grid_id,
             |station_code,
             |station_name,
             |mergeJson(data,datas) as data,
             |published_at
             |from
             |(
             |select
             |grid_id,
             |station_code,
             |station_name,
             |data,
             |datas,
             |published_at
             |from
             |(select
             |   df1.grid_id as grid_id,
             |   df1.station_code as station_code,
             |   df1.station_name as station_name,
             |   df1.data as  data,
             |   df2.data as datas,
             |   df1.published_at as published_at
             |from df1
             |JOIN df2
             |   ON  df1.grid_id = df2.grid_id
             |   AND df1.station_code = df2.station_code
             |   AND df1.station_name = df2.station_name
             |   AND df1.published_at = df2.published_at
             |)t1
             |)t2
             |""".stripMargin).select("grid_id", "station_code", "station_name", "data", "published_at")
     */
      //          val d3 = spark.sql(
      //        """
      //          |select
      //          |grid_id,
      //          |station_code,
      //          |station_name,
      //          |data,
      //          |datas,
      //          |published_at
      //          |from
      //          |(select
      //          |   df1.grid_id as grid_id,
      //          |   df1.station_code as station_code,
      //          |   df1.station_name as station_name,
      //          |   df1.data as  data,
      //          |   df2.data as datas,
      //          |   df1.published_at as published_at
      //          |from df1
      //          |JOIN df2
      //          |   ON  df1.grid_id = df2.grid_id
      //          |   AND df1.station_code = df2.station_code
      //          |   AND df1.station_name = df2.station_name
      //          |   AND df1.published_at = df2.published_at
      //          |)t1
      //        """)


      //    val sql =
      //      """
      //        |select
      //        | grid_id,
      //        | station_code,
      //        | station_name,
      //        | json_merge(t1.datas,t1.data) as data,
      //        | published_at
      //        |from
      //        |(
      //        |select
      //        |       df1.grid_id as grid_id,
      //        |       df1.station_code as station_code,
      //        |       df1.station_name as station_name,
      //        |       df1.data as data,
      //        |       df2.data as datas,
      //        |       df1.published_at as published_at
      //        |     from df1
      //        |
      //        |     JOIN df2
      //        |       on df1.station_code = df2.station_code
      //        |       and df1.published_at = df2.published_at
      //        |)t1
      //        |where
      //        |      station_code = t1.station_code
      //        |  and published_at = t1.published_at
      //        |""".stripMargin
      dataFrame1.select("grid_id", "station_code", "station_name", "data", "published_at")
        .coalesce(1)
        .foreachPartition((partition: Iterator[Row]) => {
          var connect: Connection = null
          var pstmt: PreparedStatement = null
          try {
            connect = JDBCUtils.getConnection
            // 禁用自动提交
            connect.setAutoCommit(false)
            val sql = "REPLACE INTO `alpha-center`.`station_portrait_feature`(grid_id,station_code,station_name,data,published_at) VALUES(?, ?, ?, ?, ?)"
//            val sql =
//              """
//                |insert into
//                |`alpha-center`.`station_portrait_feature`
//                |(grid_id,station_code,station_name,data,published_at)
//                |VALUES(?, ?, ?, ?, ?)
//                |on duplicate key update data = json_merge_patch(?, data)
//                |""".stripMargin
            pstmt = connect.prepareStatement(sql)
            partition.foreach(x => {
              pstmt.setString(1, x.getString(0))
              pstmt.setString(2, x.getString(1))
              pstmt.setString(3, x.getString(2))
              pstmt.setString(4, x.getString(3))
              pstmt.setString(5, x.getString(4))
//              pstmt.setString(6, x.getString(3))
              // 加入批次
              pstmt.addBatch()
            })
            // 提交批次
            pstmt.executeBatch()
            connect.commit()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            JDBCUtils.closeConnection(connect, pstmt)
          }
        })
    }

    def getBetweenDates(start_time: String, end_time: String, end: String): Unit = {
      //解析
      var a = 0
      breakable {
        while (true) {
          val start1 = getHoursTime(start_time, a)
          val published_at1 = getHoursTime(start_time, a + 2)
          val end1 = getHoursTime1(end, a)
          val published_date1 = published_at1.substring(0, 10)

//                        val published_at = "2023-01-31 22:00:00"
//                        val end = "2023-01-31 22:59:59"
//                        val start = "2023-01-31 20:00:00"
//                        val published_date = "2023-01-31"
//                        val publish_date = "2023-01-31"

          val data = recordMetaData(published_at1, end1, start1, published_date1,"00")
          val reader: DataFrame = readerMetaData(published_at1)
          println("当前执行：" + published_at1)
          val parsedData = reader.select("station_code", "station_name", "data")
            .withColumn("parsed_data", from_json(col("data"), MapType(StringType, StringType)))
            .select("station_code", "station_name", "parsed_data")
          val explodedData = parsedData.select(col("station_code"), col("station_name"), explode(col("parsed_data")))
            .select(col("station_code"), col("station_name"), col("key"), col("value").cast(StringType))

          //          val groupedData = explodedData.groupBy(col("station_code"), col("station_name"))
//            .agg(count("key").as("num_fields"))
//          val long1 = groupedData.select(col("num_fields"))
//            .agg(sum("num_fields")).head().getLong(0)

          val parsedData1 = data.select("station_code", "station_name", "data")
            .withColumn("parsed_data", from_json(col("data"), MapType(StringType, StringType)))
            .select("station_code", "station_name", "parsed_data")
          val explodedData1 = parsedData1.select(col("station_code"), col("station_name"), explode(col("parsed_data")))
            .select(col("station_code"), col("station_name"), col("key"), col("value").cast(StringType))

//          val groupedData1 = explodedData1.groupBy(col("station_code"), col("station_name"))
//                  .agg(count("key").as("num_fields"))
//          val long2 = groupedData1.select(col("num_fields"))
//            .agg(sum("num_fields")).head().getLong(0)

          if (explodedData1.except(explodedData).count() != 0) {
//            if (long1>=long2){
//              println("数据相同 ")
//            }else {
              println("补数据")
              truck_road(data)
//            }
          } else {
            println("数据相同 ")
          }
          a = a + 1
          if (start1 == end_time) {
            break()
          }
        }
      }
    }


    spark.stop()
  }

}