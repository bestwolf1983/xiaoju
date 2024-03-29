package com.xiaoju

import java.io.{BufferedReader, File, FileReader}
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by cenyuhai on 2015/8/28.
 */
object TestSparkCreateAndInsertPhoenix {


  def createTable(conn: Connection, tableName: String, cols: Int): Unit = {
    var colsStrBuild = new StringBuilder(",")
    var i = 0
    while(i < cols) {
      colsStrBuild.append(s"test${i} INTEGER,")
      i = i + 1
    }

    var createTableSql = s""" CREATE TABLE ${tableName} (
    create_date INTEGER not null, order_id bigint(20) not null, order_type INTEGER, pid bigint(20),
    city_id bigint(20),timeout bigint(20),p_phone_num varchar(20),p_create_time varchar,
    p_setup_time varchar,p_ip varchar(100),p_imei varchar(50) ,p_suuid varchar(100) ,p_alipay_zhenshen varchar(128) ,p_alipay_account varchar(128) ,
    p_weixin_openid varchar(128) , p_weixin_zhenshen varchar(128) , p_order_channel bigint(20), p_datatype INTEGER, p_networktype varchar(100) ,
    p_pay_total bigint(20), p_pay_fee bigint(20), p_pay_time varchar, p_pay_type INTEGER, p_pay_ip varchar(100) , p_pay_datatype INTEGER,
    p_pay_networktype varchar(100), p_pay_alipay_zhenshen varchar(128) , p_pay_alipay_account varchar(128) , p_pay_weixin_openid varchar(128) ,
    p_pay_weixin_zhenshen varchar(128) , p_coupon_id bigint(20), p_coupon_trackid bigint(20), p_coupon_amount bigint(20),
    is_timeout INTEGER, did bigint(20), d_phone_num varchar(20) , d_route_id INTEGER, d_alipay_zhenshen varchar(128) , d_alipay_account varchar(128),
    d_weixin_zhenshen varchar(128) , d_weixin_openid varchar(128) , d_grab_time varchar, d_order_channel bigint(20), d_datatype INTEGER,
    d_networktype varchar(100) , d_imei varchar(50) , d_ip varchar(100) , d_suuid varchar , d_arrived_ip varchar(50) ,
    d_arrived_datatype INTEGER, d_arrived_networktype varchar(100), d_arrived_time varchar, order_status INTEGER,
    cancel_time varchar, contact_type INTEGER, promoter INTEGER, contact_time varchar, complain_type INTEGER, complain_time varchar,
    remark varchar(500) , var1 INTEGER,var2 varchar(50) ,create_time varchar, update_time varchar, last_edit_time varchar ${colsStrBuild.toString}
    CONSTRAINT pk PRIMARY KEY (create_date, order_id)) SALT_BUCKETS = 100, BLOOMFILTER='ROW', DATA_BLOCK_ENCODING='PREFIX_TREE', VERSIONS=1
    """
    println(createTableSql)
    var statement = conn.createStatement()
    statement.execute(createTableSql)
    println("create table success")
  }

  /**
   * 插入数据
   * @param conn jdbc连接
   * @param tableName 表名
   * @param columnCount 列的个数，插入一些列进行测试列数对查询是否有影响
   * @param times 现有数据的倍数，用现有数据构造一些数据来测试数据量对查询的影响,必须是10的公约数，比如1,2,5,10等
   */
  def insertData(iter: Iterator[String], conn: Connection, tableName: String, columnCount: Int, times: Int): Unit = {

    // region 拼接sql
    var insertSql = new StringBuilder("UPSERT INTO " + tableName)
    var lines = SfOrderSchema.schema.split("\n")
    var map = scala.collection.mutable.HashMap[String,String]()
    insertSql.append("(")
    for(line <- lines) {
      if(!line.trim.equals("")) {
        var fields = line.trim.split(" ")
        map.put(fields(0).trim, fields(1).trim)
        insertSql.append(fields(0).trim + ",")
      }
    }

    var j = 0
    while(j < columnCount) {
      insertSql.append(s"test${j},")
      j =  j + 1
    }

    if(insertSql.endsWith(",")) {
      insertSql.deleteCharAt(insertSql.length - 1)
    }
    insertSql.append(")")
    insertSql.append(" values(")
    map.foreach { x=>
      insertSql.append("?,")
    }
    if(columnCount > 0) {
      (1 to columnCount).map(x=> insertSql.append("?,"))
    }

    if(insertSql.endsWith(",")) {
      insertSql.deleteCharAt(insertSql.length - 1)
    }
    insertSql.append(")")
    // endregion
    println(insertSql.toString())

    val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    var end: Long = 0L
    var tempString = ""
    var data: Array[String] = null
    var date: String = null
    var createDate: Date = null
    var ps = conn.prepareStatement(insertSql.toString)
    var i = 0
    var line = 1
    var start: Long = System.currentTimeMillis
    var vrow = 0
    var random = new Random
    println("start to insert data!")
    for(tempString <- iter) {
    //while ((tempString = reader.readLine()) != null) {
      //line = line + 1
      data = tempString.split("\t")
      createDate = timeFormat.parse(data(data.length - 1))
      date = dateFormat.format(createDate)

      if(times > 0) {
        vrow = 0
        // 造数据
        while(vrow < times) {
          ps.setInt(1, date.toInt)
          i = 0
          for(item <- data) {
            // skip the first col
            if(i > 0) {
              if(lines(i).contains("bigint")) {
                if(i == 1) {
                  ps.setLong(i + 1, item.trim.toLong + vrow * 100000000)
                } else {
                  ps.setLong(i + 1, item.trim.toLong)
                }
              } else if(lines(i).contains("varchar")) {
                  ps.setString(i + 1, item.trim)
              } else if(lines(i).contains("INTEGER")) {
                ps.setInt(i + 1, item.trim.toInt)
              }
            }
            i = i + 1
          }

          // 补充列数
          var n = 0
          if(columnCount > 0) {
            while(n < columnCount) {
              ps.setInt(i + 1 + n, random.nextInt())
              n = n + 1
            }
          }
          ps.addBatch()
          line = line + 1
          vrow = vrow + 1

          if (line % 10000 == 0) {
            // println("start a batch")
            try {
              println("insert 1w records")
              ps.executeBatch
              conn.commit
              System.gc()
            } catch {
              case e: Exception =>
                e.printStackTrace()
            }
            //if(line % 50000 == 0) {
              end = System.currentTimeMillis
              println("insert 1w records use " + (end - start) / 1000 + " s")
              start = end
            //}
          }
        }
      }

/*      if(line % 10000 == 0) {

      }*/
    }
    ps.executeBatch()
    conn.commit()

  }

  def main(args:Array[String]): Unit = {

    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    println("start to connect phoenix!")
    var createTableConn = DriverManager.getConnection("jdbc:phoenix:bigdata-arch-hdp277.bh:2181")
    println("connected to  phoenix!")
    /*println("table name: " + args(0))
    println("column size: " + args(1))
    println("times: " + args(2))*/
    createTable(createTableConn, "SF_ORDER2",1000)
    var sparkConf = new SparkConf().setAppName("spark phoenix")
    var sc = new SparkContext(sparkConf)
    var inputfile = sc.textFile("/output.txt")
    inputfile.foreachPartition{iter=>
      //Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      println("for each partition!")
      var conn = DriverManager.getConnection("jdbc:phoenix:bigdata-arch-hdp277.bh:2181")
      insertData(iter, conn, "SF_ORDER2", 1000, 1)
    }


    // Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver")
    //var conn = DriverManager.getConnection("jdbc:phoenix:spark80.qq,spark85.qq,spark86.qq:3333")
/*    var lines = SfOrderSchema.schema.split("\n")
    var map = new java.util.HashMap[String,String]()
    for(line <- lines) {
      if(!line.trim.equals("")) {
        var fields = line.trim.split(" ")
        println(fields(0).trim + ":" + fields(1).trim)
        map.put(fields(0).trim, fields(1).trim)
      }
    }*/

  }

}
