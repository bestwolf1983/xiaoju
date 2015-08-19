package com.xiaoju


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.sql.{DriverManager, PreparedStatement}

import java.util.HashMap

object TestDmInsert {
  val dbClassName = "io.crate.client.jdbc.CrateDriver"
  val CONNECTION = "crate://localhost:4300"


  def insert(split: String) {
    try {
      val conn = DriverManager.getConnection(CONNECTION)

      var tempString: String = null
      var line = 1
      var insertSql: StringBuilder = null
      var ps: PreparedStatement = null
      var fields = TableSchema.fieldSchema.split("\n")
      var map = new HashMap[Integer, String]()
      var temp: String = null
      var i = 0
      while (i < fields.length) {
        temp = fields(i).split("\t")(1)
        map.put(i, temp.trim())
        i = i + 1
      }

      var fieldLength = fields.length

      insertSql = new StringBuilder(TableSchema.insertBiTagSql)
      insertSql.append("(")

      i = 0
      while (i < fieldLength) {
        insertSql.append("?,")
        i = i + 1
      }

      if (insertSql.lastIndexOf(",") > -1) {
        insertSql = insertSql.deleteCharAt(insertSql.length - 1)
      }

      insertSql.append(")")

      var data: Array[String] = null
      ps = conn.prepareStatement(insertSql.toString())
      var start = System.currentTimeMillis()
      var end = 0L

      var reader: BufferedReader = null
      var fs = FileSystem.get(new Configuration())
      var files = fs.listStatus(new Path("/user/rd/bi_dm/tmp_dm_tag_pass_dd_kd_merge_1/year=2015/month=07/day=06/pidsn=-1"))

      var startFileIndex = 0
      if (split.equals("1")) {
        startFileIndex = 0
      } else if (split.equals("2")) {
        startFileIndex = 40
      } else if (split.equals("3")) {
        startFileIndex = 80
      }

      var endFileIndex = startFileIndex + 40
      if (endFileIndex > files.length) {
        endFileIndex = files.length
      }

      i = startFileIndex
      var j = 0
      while (i < endFileIndex) {
        var in: InputStream = null
        try {
          in = fs.open(files(i).getPath())
          reader = new BufferedReader(new InputStreamReader(in))
          while ((tempString = reader.readLine()) != null) {
            line = line + 1
            data = tempString.split("\t")
            j = 0
            while(j < data.length) {
              temp = map.get(j).trim()
              if (temp.startsWith("int")) {
                ps.setInt(j + 1, data(i).trim().replaceAll("'", "").toInt)
              } else if (temp.startsWith("double")) {
                ps.setDouble(j + 1, data(i).trim().replaceAll("'", "").toDouble)
              } else {
                ps.setString(j + 1, data(i).trim().replaceAll("'", ""))
              }
              j = j + 1
            }

            ps.addBatch()

            if (line % 10000 == 0) {
              ps.executeBatch()
              conn.commit()
              end = System.currentTimeMillis()
              println("insert 1w records use " + (end - start) / 1000 + " s")
              start = end
            }

            if (line % 50000 == 0) {
              println("handle " + line + " records!")
            }
          }
          ps.executeBatch()
          conn.commit()
          in.close()
          reader.close()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      }
      i = i + 1

    }


    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def main (args: Array[String]) {
    Class.forName(dbClassName)
    var start = System.currentTimeMillis()
    insert(args(0))
    var end = System.currentTimeMillis()
    println("Use " + (end - start) / 1000 + "s")
  }

}

