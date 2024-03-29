package com.xiaoju

import java.sql.DriverManager

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Created by cenyuhai on 2015/8/31.
 */
object TestPhoenixSql {

  def testParalism(sql: String, paralism: Int): Unit = {
    var startTime = System.currentTimeMillis()
    var threads = new Array[Thread](paralism)
    (0 until paralism).foreach { i=>
      threads(i) = new Thread() {
        override def run(): Unit = {
          var start = System.currentTimeMillis()
          var conn = DriverManager.getConnection("jdbc:phoenix:bigdata-arch-hdp277.bh:2181")
          var end = System.currentTimeMillis()
          println("connect to server use " + (end - start) + " ms")
          start = end
          var statement = conn.createStatement()
          end = System.currentTimeMillis()
          println("create statment use " + (end - start) + " ms")
          start = end

          var result = statement.executeQuery(sql)
          var meta = result.getMetaData()
          var cols = meta.getColumnCount()
          var i = 0

          while(result.next()) {
            println("result: " + result.getLong(1))
            i = i + 1
          }
          var execEnd = System.currentTimeMillis()
          println("execute query use " + (execEnd - start) + " ms")
          statement.close()
          conn.close()
        }
      }
    }
    threads.foreach(_.start())
    threads.foreach(_.join())
    var endTime = System.currentTimeMillis()

    println("totally use " + (endTime - startTime)  + " ms")
  }

  def main(args:Array[String]): Unit = {
    testParalism(args(0), args(1).toInt)
  }
}
