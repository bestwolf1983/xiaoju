package com.xiaoju

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI
import java.sql.{DriverManager, PreparedStatement}
import java.util.{UUID, Date, HashMap}
import scala.reflect.runtime.universe._
import io.crate.client.CrateClient
import scala.collection.JavaConverters._
import io.crate.action.sql.SQLBulkRequest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object TestDmBuilkInsert {
  val dbClassName = "io.crate.client.jdbc.CrateDriver"
  val CONNECTION = "crate://localhost:4300"

  def convertToJavaColumnType(o: Any): Object = {
    if(o == null) return null
    o match {
      case x:java.lang.Long => x
      case x:java.lang.Double => x
      case x:java.lang.Integer => x
      case x:Int => x.asInstanceOf[java.lang.Integer]
      case x:Long => x.asInstanceOf[java.lang.Long]
      case x:Double => x.asInstanceOf[java.lang.Double]
      case x: String => x
      case x: Array[Short] => x.map(_.asInstanceOf[java.lang.Short])
      case x: Array[Int] => x.map(_.asInstanceOf[java.lang.Integer])
      case x: Array[Long] => x.map(_.asInstanceOf[java.lang.Long])
      case x: Array[Float] => x.map(_.asInstanceOf[java.lang.Float])
      case x: Array[Double] => x.map(_.asInstanceOf[java.lang.Double])
      case x: Array[Byte] => x.map(_.asInstanceOf[java.lang.Byte])
      case x: Array[Boolean] => x.map(_.asInstanceOf[java.lang.Boolean])
      case x: Array[Any] => x.map(_.asInstanceOf[java.lang.Object])
      case m: Map[_, _] => m.asJava
      case t: Seq[_] => t.asJava
      case s: Some[_] => convertToJavaColumnType(s.get)
      case None => null
      case d: Date => d.getTime().asInstanceOf[java.lang.Long]
      case u: UUID => u.toString()
      case v: Any => v.asInstanceOf[AnyRef]
    }
  }

  def insert(split: String) {
    try {
      var client = new CrateClient(
        "spark79.qq:4300",
        "spark85.qq:4300",
        "spark86.qq:4300"
      )

      var tempString: String = null
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
      var start = System.currentTimeMillis()
      var end = 0L

      var reader: BufferedReader = null
      var fileDir = "hdfs://mycluster/user/rd/bi_dm/tmp_dm_tag_pass_dd_kd_merge_1/year=2015/month=07/day=06/pidsn=-1/"
      var conf = new Configuration()
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem")
      var fs = FileSystem.get(URI.create(fileDir), conf)
      var files = fs.listStatus(new Path(fileDir))

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

      var bulkArgs = Array.ofDim[Any](1000, fieldLength)
      var param:  Array[Any] = null
      i = startFileIndex
      var j = 0
      while (i < endFileIndex) {
        var in: InputStream = null
        try {
          in = fs.open(files(i).getPath())
          reader = new BufferedReader(new InputStreamReader(in))
          var line = 0
          while ((tempString = reader.readLine()) != null) {
            line = line + 1
            data = tempString.split("\t")
            j = 0
            param = new Array[Any](fieldLength)
            while(j < data.length) {
              temp = map.get(j).trim()
              if (temp.startsWith("int")) {
                temp = data(i).trim().replaceAll("'", "")
                if(temp == "\\N") {
                  param(j) = null
                } else {
                  param(j) = data(i).trim().replaceAll("'", "").toLong
                }
              } else if (temp.startsWith("double")) {
                temp = data(i).trim().replaceAll("'", "")
                if(temp == "\\N") {
                  param(j) = null
                } else {
                  param(j) = data(i).trim().replaceAll("'", "").toDouble
                }

              } else {
                param(j) = data(j).trim().replaceAll("'", "")
              }
              j = j + 1
            }
            bulkArgs((line-1) % 1000) = param

            if (line % 1000 == 0) {
              val javaArgs = bulkArgs.map(_.map(convertToJavaColumnType(_)))
              var request = new SQLBulkRequest(insertSql.toString(), javaArgs)
              client.bulkSql(request).actionGet()
              bulkArgs = Array.ofDim[Any](1000, fieldLength)
            }

            if(line % 10000 == 0) {
              end = System.currentTimeMillis()
              println("insert 1w records use " + (end - start) / 1000 + " s")
              start = end
            }

            if (line % 100000 == 0) {
              println("handle " + line + " records!")
            }
          }

          in.close()
          reader.close()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
        i = i + 1
      }
    }

    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }


  def main (args: Array[String]) {
    // Class.forName(dbClassName)
    var start = System.currentTimeMillis()
    insert(args(0))
    var end = System.currentTimeMillis()
    println("Use " + (end - start) / 1000 + "s")
  }

}

