package com.xiaoju

import java.io._
import java.net.URI
import java.sql.PreparedStatement
import java.util.{Date, HashMap, UUID}

import io.crate.action.sql.SQLBulkRequest
import io.crate.client.CrateClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._


object TestDmLocalInsert {
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
      case x: Array[String] => x
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
      var fileDir = "/data/xiaoju/soft/cenyuhai/data/"
      var files = new File(fileDir).listFiles()

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
        // var in: InputStream = null
        try {
          //in = fs.open(files(i).getPath())
          reader = new BufferedReader(new FileReader(files(i)))
          var line = 0
          while ((tempString = reader.readLine()) != null) {
            line = line + 1
            data = tempString.split("\t")
            j = 0
            param = new Array[Any](fieldLength)
            while(j < data.length) {
              temp = map.get(j).trim()
              if (temp.startsWith("int")) {
                temp = data(j).trim().replaceAll("'", "")
                if(temp == "\\N") {
                  param(j) = null
                } else {
                  param(j) = data(j).trim().replaceAll("'", "").toInt
                }
              } else if (temp.startsWith("double")) {
                temp = data(j).trim().replaceAll("'", "")
                if(temp == "\\N") {
                  param(j) = null
                } else {
                  param(j) = data(j).trim().replaceAll("'", "").toDouble
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

          // in.close()
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

