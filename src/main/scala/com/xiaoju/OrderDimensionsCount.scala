package com.xiaoju


import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, Properties}
import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
 * Created by cenyuhai on 15-4-14.
 */
object OrderDimensionsCount {

  var checkpointDirectory = ""
  var sourceTopic = ""
  var outputTopic = ""
  var sourceBrokerList = ""
  var sourceZookeeper = ""
  var outputBrokerList = ""
  var groupId = ""
  var timestampColName = "timestamp"
  var dimensions = ""
  var parallelism = 1
  var streamType  = ""
  var dataSchema = ""

  def filterByTime(orderTime: String, jobTime: Long, timeType: Int, dateFormat: SimpleDateFormat): Boolean = {
    try {
      var startDate = dateFormat.parse(orderTime)
      var endDate = new Date(jobTime)
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

      if (elapsedDays == 0 && elapsedHours == 0) {
        if (elapsedMinutes == 0 && elapsedSeconds == 0) return false

        if (elapsedMinutes >= 0 && elapsedMinutes < timeType)
          return true
        else if (elapsedMinutes == timeType && elapsedSeconds == 0)
          return true
      }
    } catch {
      case e: Exception =>
    }

    return false

  }

  def handlePartition(partitionOfRecords: Iterator[(String, Iterable[String])], timeType: Int, time: Time, dimensions: String, outputTopic: String, brokerList: String) {
    var producer: KafkaProducer = null
    try {
      producer = new KafkaProducer(outputTopic, brokerList)

      partitionOfRecords.foreach(x => {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        var date = dateFormat.format(new Date(time.milliseconds))
        var count = new AtomicLong(0)
        x._2.foreach(orderTime =>
          if (filterByTime(orderTime, time.milliseconds, timeType, dateFormat)) {
            count.incrementAndGet()
          }
        )

        if (count.get() > 0) {
          var json = new mutable.StringBuilder("{\"timestamp\":\"" + date + "\",")
          val values = x._1.split("_")
          var i = 0
          var dimensionArray = dimensions.split(",")
          for (dimension <- dimensionArray) {
            json.append("\"" + dimension + "\":\"" + values(i) + "\",")
            i = i + 1
          }
          json.append("\"count\":" + count.get() + "}")
          producer.send(json.toString())
        }
      })
      producer.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        if(producer != null) {

        }
      }
    }
  }

  def createStreamingContext(): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("OrderDimensionsCount")
    sparkConf.set("spark.default.parallelism", parallelism.toString)
    // sparkConf.set("spark.streaming.blockInterval", "1000")
    // sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConf.set("dimemsion", dimensions)
    sparkConf.set("outputTopic", outputTopic)
    sparkConf.set("brokers", outputBrokerList)

    println("Dimension in streaming context is " + dimensions)
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicsSet = Set(sourceTopic)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> sourceBrokerList,
      "group.id" -> groupId
    )
    var messages :DStream[(String, String)] = null
    val topicsMap = Map[String, Int](sourceTopic -> 1)
    if(streamType == "true") {
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    } else {
      val kafkaStreams = (1 to parallelism).map { i => KafkaUtils.createStream(ssc, sourceZookeeper, groupId, topicsMap) }
      messages = ssc.union(kafkaStreams)
    }

    val cacheOrder = messages.map(msg => formatMessage(msg._2)).filter(o => o != null)

    var oneOrder = cacheOrder.window(Seconds(120), Seconds(60))

    def handleWindowData(dataStream: DStream[(Long, java.util.Map[String, String])], timeType: Int): Unit = {
      var dimensions = dataStream.context.sparkContext.getConf.get("dimemsion")
      var brokerList = dataStream.context.sparkContext.getConf.get("brokers")
      var outputTopic = dataStream.context.sparkContext.getConf.get("outputTopic")
      dataStream.map(x => createGroup(x._2, dimensions)).groupByKey().foreachRDD((rdd, time) => {
        try {
          if (!rdd.partitions.isEmpty) {
            rdd.foreachPartition(partition => {
              handlePartition(partition, timeType, time, dimensions, outputTopic, brokerList)
            })
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      })
    }

    handleWindowData(oneOrder, 1)

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def createGroup(order: java.util.Map[String, String], dimensions: String): (String, String) = {
    var dimensionArray = dimensions.split(",")
    val orderTime = order.get(timestampColName)
    var key = ""
    for (dimension <- dimensionArray) {
      key = key + "_" + order.get(dimension)
    }
    (key.substring(1, key.length), orderTime)
  }

  /*  def createGroup(order: mutable.HashMap[String, String]): Array[(String, (Int, String))] = {
      val orderTime = order(timestampColName)
      constructGroup(groups, orderTime, dimensionMap, order)
    }*/

  def constructGroup(groups: ListBuffer[String],
                     orderTime: String,
                     dimensionMap: Map[String, String],
                     order: mutable.HashMap[String, String]): Array[((String, String), (Int, String))] = {
    val dimensionCount = dimensionMap.size
    var items: Array[String] = null
    var results = new ArrayBuffer[((String, String), (Int, String))]()
    var i = 1
    for (group <- groups) {
      i = 1
      var value = ""
      var name = ""
      if (group.contains(",")) {
        items = group.split(",")
        while (i <= dimensionCount) {
          if (items.contains(i.toString)) {
            value = value + "_" + order(dimensionMap(i.toString))
            name = name + "" + "_" + dimensionMap(i.toString)
          } else {
            value = value + "_*"
            name = name + "_*"
          }
          i = i + 1
        }
      } else {
        val number = group.toInt
        while (i <= dimensionCount) {
          if (i == number) {
            value = value + "_" + order(dimensionMap(i.toString))
            name = name + "" + "_" + dimensionMap(i.toString)
          } else {
            value = value + "_*"
            name = name + "_*"
          }
          i = i + 1
        }
      }
      results += (((name.substring(1, name.length), value.substring(1, value.length)), (1, orderTime)))

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

    checkpointDirectory = config.getProperty("CheckpointDirectory").trim
    sourceTopic = config.getProperty("SourceTopic").trim
    outputTopic = config.getProperty("OutputTopic").trim
    parallelism = config.getProperty("Parallelism").trim.toInt
    sourceZookeeper = config.getProperty("SourceZookeepers").trim
    sourceBrokerList = config.getProperty("SourceBrokers").trim
    outputBrokerList = config.getProperty("OutputBrokers").trim
    groupId = config.getProperty("ConsumerGroup").trim
    dimensions = config.getProperty("Dimensions").trim
    streamType = config.getProperty("UseDirectApi").trim

    println("Checkpoint:" + checkpointDirectory)
    println("Source Topic:" + sourceTopic)
    println("Output Topic:" + outputTopic)
    println("Source Zookeeper:" + sourceZookeeper)
    println("Source brokers:" + sourceBrokerList)
    println("Output brokers:" + outputBrokerList)
    println("Consumer group:" + groupId)
    println("Dimension:" + dimensions)
  }


  def formatMessage(message: String): (Long, java.util.Map[String, String]) = {
    var orderOption = parseToMap(message)
    if (orderOption == None) {
      null
    } else {
      try {
        val order = orderOption.get
        val orderId = order.get("orderId").toLong
        (orderId, order)
      } catch {
        case e: Exception => null
      }
    }
  }

  def parseToMap(message: String): Option[java.util.Map[String, String]] = {
    try {
      val jsonObj = JSON.parseObject(message)
      Some(jsonObj.asInstanceOf[java.util.Map[String, String]])
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  def main(args: Array[String]): Unit = {
    val config = new Properties()
    if (args.length == 1) {
      val in = new FileInputStream(args(0))
      config.load(in)
      readProperties(config)
      in.close()
      println("read properties from file:" + args(0))
    } else {
      /*      val in = this.getClass.getResourceAsStream("/order.properties")
            config.load(in)
            readProperties(config)
            in.close()*/
      println("please specify the properties file path")
    }


    var ssc: StreamingContext = null
    try {
      ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createStreamingContext())
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Exception =>
        println(e.getMessage)
        ssc.stop()
    }


  }
}


