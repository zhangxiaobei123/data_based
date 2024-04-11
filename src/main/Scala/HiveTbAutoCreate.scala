//import org.apache.log4j.{Level, Logger}

import java.util
import java.sql.{Connection, PreparedStatement, ResultSet}

object HiveTbAutoCreate {



  def main(args: Array[String]): Unit = {

//    from_dm_to_create("CTWRY_DATA.T_AUTO_AIR_REALTIME_DATA_SOURCE")
  }
  /*def from_dm_to_create (db_tb_name: String)={
//    System.setProperty("hadoop.home.dir", "E:\\software\\winutil\\")
//    val logger = Logger.getLogger("Dm2Hive-log")
//    logger.setLevel(Level.WARN)
//    val db_tb_name = db_tb_name_list(0)
    val db_tb = db_tb_name.split("\\.")
    val db_name = db_tb(0)
    val db_name_hive = db_tb(0).toLowerCase
    val tb_name = db_tb(1)
    val tb_name_hive = db_tb(1).replaceAll("_SOURCE$","").toLowerCase
    //查询对应数据表的注解
    val tb_comment_sql = s"select COMMENTS from USER_TAB_COMMENTS where TABLE_NAME='$tb_name'"
    val comment_query_func=(sql:String)=>{
      var conn: Connection = null
      var ps: PreparedStatement = null
      var rs: ResultSet = null
      var comment:Option[String] = None
      try {
        conn = JDBCUtils.getConnection
        ps = conn.prepareStatement(sql)
        //查询获取结果集
        rs = ps.executeQuery
        //获取结果集
        while ( rs.next) comment = Some(rs.getString(1))
      } catch {
        case e: Exception => e.printStackTrace()
      } finally JDBCUtils.closeConnection(conn,ps)
      comment
    }

    val comment = comment_query_func(tb_comment_sql) match {
      case Some(x) => x+""
      case None => ""
    }

    val column_sql = s"select columns.COLUMN_NAME,columns.DATA_TYPE,columns.DATA_LENGTH,comm.COMMENTS from " +
      s"(select COLUMN_NAME,DATA_TYPE,DATA_LENGTH from user_tab_columns where table_name='$tb_name') columns left join " +
      s"(select COLUMN_NAME,COMMENTS from user_col_comments where table_name='$tb_name') comm  on comm.COLUMN_NAME=columns.COLUMN_NAME"
    //查询字段、字段类型、字段长度
    val column_query_func=(sql:String)=> {
      var conn: Connection = null
      var ps: PreparedStatement = null
      var rs: ResultSet = null
      val arrayList: util.ArrayList[(String,String,String,String)] = new util.ArrayList[(String,String,String,String)]
      try {
        conn = JDBCUtils.getConnection
        ps = conn.prepareStatement(sql)

        //查询获取结果集
        rs = ps.executeQuery
        //获取结果集
        while ( rs.next) arrayList.add(rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally JDBCUtils.closeConnection(conn,ps)
      arrayList
    }

    //hive库创建语句
    //    val db_sql = s"create database ods_$db_name_hive;"
    //hive表创建语句
    var tb_sql=s"create external table ods_$db_name_hive.ods_$tb_name_hive(\n"

    val columns = column_query_func(column_sql)
    println(columns.size())
    if (!columns.isEmpty()) {
      for(i <- 0 until columns.size()){
        val data = columns.get(i)
        val column_name = data._1
        val column_type = data._2.toLowerCase
        val column_length = data._3
        val column_comment = data._4
        if (i != columns.size()-1) {
          //由于hive不存在DATATIME、DATA、TIME数据类型，需要做类型转换
          column_type match {
            case "integer" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" int,\n"
                case _ => tb_sql = tb_sql+column_name+" int "+s"comment '$column_comment',\n"
              }
            }
            case "int" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" int,\n"
                case _ => tb_sql = tb_sql+column_name+" int "+s"comment '$column_comment',\n"
              }
            }
            case "double" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" double,\n"
                case _ => tb_sql = tb_sql+column_name+" double "+s"comment '$column_comment',\n"
              }
            }
            case "char" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" varchar("+column_length+"),\n"
                case _ => tb_sql = tb_sql+column_name+" varchar("+column_length+s") comment '$column_comment',\n"
              }
            }
            case "datetime" =>{
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" string,\n"
                case _ => tb_sql = tb_sql+column_name+" string "+s"comment '$column_comment',\n"
              }
            }
            case _ =>  {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" "+column_type+"("+column_length+"),\n"
                case _ => tb_sql = tb_sql+column_name+" "+column_type+"("+column_length+s") comment '$column_comment',\n"
              }
            }
          }
        }else{
          column_type match {
            case "integer" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+s" int)comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
                case _ =>tb_sql = tb_sql+column_name+" int"+s" comment '$column_comment')comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
              }
            }
            case "int" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+s" int)comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
                case _ =>tb_sql = tb_sql+column_name+" int"+s" comment '$column_comment')comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
              }
            }
            case "double" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+s" double)comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
                case _ =>tb_sql = tb_sql+column_name+" double"+s" comment '$column_comment')comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
              }
            }
            case "datetime" => {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+s" string)comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
                case _ =>tb_sql = tb_sql+column_name+" string"+s" comment '$column_comment')comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
              }
            }
            case _ =>  {
              column_comment match {
                case null => tb_sql = tb_sql+column_name+" "+column_type+"("+column_length+s"))comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
                case _ => tb_sql = tb_sql+column_name+" "+column_type+"("+column_length+s") comment '$column_comment')comment '$comment' STORED AS PARQUET \n"+
                  s"location 'hdfs://GlztCluster/warehouse/external/ods_$db_name_hive/ods_$tb_name_hive' tblproperties('parquet.compress'='SNAPPY');"
              }
            }
          }
        }
      }
    }

    println(tb_sql)*/
//  }
}
