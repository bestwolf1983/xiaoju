package com.xiaoju

import java.io.{File, FileReader, BufferedReader}
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.regionserver.ColumnCount

import scala.util.Random

/**
 * Created by cenyuhai on 2015/8/28.
 */
object TestInsertPhoenix {
  /**
   * 插入数据
   * @param conn jdbc连接
   * @param tableName 表名
   * @param columnCount 列的个数，插入一些列进行测试列数对查询是否有影响
   * @param times 现有数据的倍数，用现有数据构造一些数据来测试数据量对查询的影响,必须是10的公约数，比如1,2,5,10等
   */
  def insertData(conn: Connection, tableName: String, columnCount: Int, times: Int): Unit = {

    // region 拼接sql
    var insertSql = new StringBuilder("UPSERT INTO " + tableName)
    var lines = SfOrderSchema.schema.split("\n")
    var map = scala.collection.mutable.HashMap[String,String]()

    for(line <- lines) {
      if(!line.trim.equals("")) {
        var fields = line.trim.split(" ")
        map.put(fields(0).trim, fields(1).trim)
      }
    }
    insertSql.append("(")
    map.foreach { x=>
      insertSql.append(x._1.trim + ",")
    }
    if(insertSql.endsWith(",")) {
      insertSql.deleteCharAt(insertSql.length - 1)
    }
    insertSql.append(")")
    insertSql.append(" values(")
    map.foreach { x=>
      insertSql.append("?,")
    }
    if(insertSql.endsWith(",")) {
      insertSql.deleteCharAt(insertSql.length - 1)
    }
    insertSql.append(")")
    // endregion

    println(insertSql.toString())

    val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    val fileName: String = "/data/xiaoju/output.txt"
    val file: File = new File(fileName)
    var reader = new BufferedReader(new FileReader(file))
    var end: Long = 0L
    var tempString = ""
    var data: Array[String] = null
    var date: String = null
    var createDate: Date = null
    var ps = conn.prepareStatement(insertSql.toString)
    var i = 0
    var line = 0
    var start: Long = System.currentTimeMillis
    var vrow = 0
    var random = new Random
    println("start to insert data!")
    while ((tempString = reader.readLine()) != null) {
      line = line + 1
      data = tempString.split("\t")
      createDate = timeFormat.parse(data(data.length - 1))
      date = dateFormat.format(createDate)

      if(times > 0) {
        vrow = 0
        // 造数据
        while(vrow < times) {
          ps.setLong(1, date.toLong)
          i = 1
          for(item <- data) {
            if(lines(i).contains("bigint")) {
              if(i == 2) {
                ps.setLong(i + 1, item.trim.toLong + vrow * 100000000)
              } else {
                ps.setLong(i + 1, item.trim.toLong)
              }

            } else if(lines(i).contains("varchar")) {
              ps.setString(i + 1, item.trim)
            } else if(lines(i).contains("INTEGER")) {
              ps.setInt(i + 1, item.trim.toInt)
            }
            i = i + 1
          }

          // 补充列数
          if(columnCount > 0) {
            while(i < columnCount) {
              ps.setLong(i, random.nextLong())
              i = i + 1
            }
          }
          ps.addBatch()
        }

      }

      if (line % 10000 == 0) {
        ps.executeBatch
        conn.commit
        end = System.currentTimeMillis
        println("insert 1w records use " + (end - start) / 1000 + " s")
        start = end
      }
    }
    ps.executeBatch()
    conn.commit()

  }



  def main(args:Array[String]): Unit = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    println("start to connect phoenix!")
    var conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181")
    println("connected to  phoenix!")
    insertData(conn, args(0), args(1).toInt, args(2).toInt)

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
