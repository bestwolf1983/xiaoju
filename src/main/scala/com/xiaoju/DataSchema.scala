package com.xiaoju

import java.io.FileInputStream
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}


import com.alibaba.fastjson.JSON
import org.apache.hadoop.security.UserGroupInformation
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random


/**
 * Created by cenyuhai on 2015/6/23.
 */
object DataSchema {

  val Schema =
    """
      |orderId,
      |trip_id,
      |passengerId,
      |token,
      |driverId,
      |status,
      |type,
      |lng,
      |lat,
      |address,
      |destination,
      |setuptime,
      |tip,
      |exp,
      |waittime,
      |callCount,
      |distance,
      |length,
      |verifyMessage,
      |createTime,
      |striveTime,
      |arriveTime,
      |getofftime,
      |aboardTime,
      |cancelTime,
      |pCommentGread,
      |pCommentText,
      |pCommentTime,
      |pCommentStatus,
      |dCommentGread,
      |dCommentText,
      |dCommentTime,
      |dCommentStatus,
      |channel,
      |area,
      |version,
      |remark,
      |bonus,
      |voicelength,
      |voiceTime,
      |extra_info,
      |pay_info,
      |destlng,
      |destlat,
      |srclng,
      |order_terminate_pid,
      |srclat
    """.stripMargin


  def constructGroup(groups: ListBuffer[String],
                     orderTime: String,
                     dimensionMap: Map[String, String],
                     order: mutable.HashMap[String, String]): Array[(String, (Int, String))] = {
    val dimensionCount = dimensionMap.size
    var items: Array[String] = null
    var results = new ArrayBuffer[(String, (Int, String))]()
    var i = 1
    for (group <- groups) {
      i = 1
      var result = orderTime
      if (group.contains(",")) {
        items = group.split(",")
        while (i <= dimensionCount) {
          if (items.contains(i.toString)) {
            result = result + "_" + order(dimensionMap(i.toString))
          } else {
            result = result + "_*"
          }
          i = i + 1
        }
      } else {
        val number = group.toInt
        while (i <= dimensionCount) {
          if (i == number) {
            result = result + "_" + order(dimensionMap(i.toString))
          } else {
            result = result + "_*"
          }
          i = i + 1
        }
      }
      results += ((result, (1, orderTime)))
    }
    results.toArray
  }

  def count(i: Int, str: String, num: Array[Int], result: ListBuffer[String]) {
    if (i == num.length) {
      if (!str.equals("")) {
        val field = str.substring(0, str.length() - 1)
        result += field
      }
      return
    }
    count(i + 1, str, num, result)
    count(i + 1, str + num(i) + ",", num, result)
  }

  def readProperties(config: Properties) = {
    var checkpointDirectory = config.getProperty("CheckpointDirectory", "/data/xiaoju/checkpoint/OrderDimensionsCount").trim
    var sourceTopic = config.getProperty("SourceTopic", "recentorder").trim
    var outputTopic = config.getProperty("OutputTopic", "OrderDimensionsCount").trim
    var brokerList = config.getProperty("Brokers", "hdp71.qq:9099,hdp72.qq:9092,hdp73.qq:9092").trim
    var groupId = config.getProperty("ConsumerGroup", "SPARK_OrderDimensionsCount").trim
    var dimensions = config.getProperty("Dimensions", "status,type,waittime").trim
    var dataSchema = config.getProperty("Schema", DataSchema.Schema).trim
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")


  def filterOneMiniteRange(orderTime: String, currentTime: Long): Boolean = {
    val currentDate = new Date(currentTime)
    val orderDate = new Date(orderTime.toLong)
    if (currentDate.getMinutes - 1 == orderDate.getMinutes
      || (currentDate.getMinutes + orderDate.getMinutes == 59)) {
      true
    } else {
      false
    }
  }

  def getDatePoor(startDate: Date, endDate: Date, timeType: Int): Boolean = {
    endDate.setSeconds(0)
    var different = endDate.getTime() - startDate.getTime()

    var secondsInMilli = 1000L
    var minutesInMilli = secondsInMilli * 60L
    var hoursInMilli = minutesInMilli * 60L
    var daysInMilli = hoursInMilli * 24L

    var elapsedDays = different / daysInMilli
    different = different % daysInMilli

    var elapsedHours = different / hoursInMilli
    different = different % hoursInMilli

    var elapsedMinutes = different / minutesInMilli
    different = different % minutesInMilli

    var elapsedSeconds = different / secondsInMilli

    println(elapsedDays + "_" + elapsedHours + "_" + elapsedMinutes + "_" + elapsedSeconds)

    if (elapsedDays == 0 && elapsedHours == 0) {
      if (elapsedMinutes == 0 && elapsedSeconds == 0) return false

      if (elapsedMinutes >= 0 && elapsedMinutes < timeType)
        return true
      else if (elapsedMinutes == timeType && elapsedSeconds == 0)
        return true
    }

    return false
  }

  def main(args: Array[String]) {
/*    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val str = "{\"timestamp\":\"2015-06-25T13:16:32\",\"db\":\"test\",\"table\":\"cc\",\"optype\":\"u\",\"id\":\"1431599\",\"age\":\"2\",\"name\":\"w\",\"tt\":\"2\"}"
    val jsonObj = JSON.parseObject(str)
    val map = jsonObj.asInstanceOf[java.util.Map[String,String]]
    println(map)*/

    val driverName = "org.apache.hive.jdbc.HiveDriver"

    Class.forName(driverName)

/*    private val thriftServers = conf.get("spark.thriftserver.hosts")
    private var server = ""
    if(thriftServers != null) {
      val servers = thriftServers.split(",")
      if(servers.length == 1) {
        server = servers(0)
      } else {
        val randomNumber = Random.nextInt(servers.length)
        server = servers(randomNumber)
      }
    }*/

    var server = "hdp1.jx:10000"

    // val currentUserName = UserGroupInformation.getCurrentUser.getUserName
    val conn = DriverManager.getConnection(s"jdbc:hive2://${server}",
      "xiaoju" , "")
    var cmd = "desc test"
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(cmd)
    val meta = resultSet.getMetaData()
    val results: ArrayBuffer[String] = new ArrayBuffer[String]()

    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")

    while(resultSet.next()) {
        if(tokens(0).equalsIgnoreCase("desc")) {
          results += Seq(resultSet.getString(1), resultSet.getString(2),
            Option(resultSet.getString(3)).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
        } else if(tokens(0).equalsIgnoreCase("select")) {

        } else {
          results += resultSet.getString(1)
        }

    }


    def close: Unit = {
      conn.close()
    }


/*    while(i < 100) {
      val randonNumber = Random.nextInt(s.length)
      println(randonNumber)
      i = i + 1
    }*/


    // val host = s(randonNumber)


    //var endDate = dateFormat.parse("2015-06-24 10:02:30")
    //var startDate = dateFormat.parse("2015-06-24 10:00:59")
    //println(endDate.toLocaleString)

    //val date = new Date()
    //println(dateFormat.format(date))
    //println(getDatePoor(startDate, endDate, 1))

    /*    val result = filterOneMiniteRange("1435058144000", 1435058160000L)
        println(result)*/
    /*    val map = new util.HashMap[String,String]()
        map.put("type", "1")

        println(JSON.toJSON(map))*/


    /*    val dimensions = "status,type,waittime"
        val dimensionArray = dimensions.split(",")
        val dimensionMap = dimensionArray.zipWithIndex.map(x=> ((x._2 + 1).toString, x._1)).toMap
        var num = (1 to dimensionArray.length).toArray
        var groups = new ListBuffer[String]()
        val orderTime = "201506231503"

        var order = new mutable.HashMap[String, String]()
        order += ("status" -> "status1")
        order += ("type" -> "type2")
        order += ("waittime" -> "waittime3")
        count(0, "", num, groups)
        val results = constructGroup(groups, orderTime, dimensionMap, order)

        for(result <- results) {
          println(result)
        }*/


    // val in = this.getClass.getResourceAsStream("/order.properties")
    /*    val in = new FileInputStream("d:/order.properties")
        val config = new Properties()
        config.load(in)
        readProperties(config)
        in.close()*/

  }
}
