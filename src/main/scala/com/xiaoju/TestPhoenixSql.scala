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
    val futureList = Future.sequence((1 to paralism).toList.map(x=>
      Future {
        var start = System.currentTimeMillis()
        var conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181")
        var end = System.currentTimeMillis()
        println("connect to server use " + (end - start) + " ms")
        start = end
        var statement = conn.createStatement()
        end = System.currentTimeMillis()
        println("create statment use " + (end - start) + " ms")
        start = end
        statement.executeQuery(sql)
        println("execute query use " + (end - start) + " ms")
        statement.close()
        conn.close()
      }
    ))

    futureList foreach println
  }

  def main(args:Array[String]): Unit = {
    testParalism(args(0), args(1).toInt)
  }
}
