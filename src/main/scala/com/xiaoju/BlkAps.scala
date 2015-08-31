package com.xiaoju

import com.google.common.hash.Hashing
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.JsonAST.{JArray, JString, JInt, JDouble}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text


trait SrcDataTypeItem {
  val time: Long
}
/*
*/
trait GpsLngLat {
  val lng: Double
  val lat: Double
}

case class SrcDataGps(time: Long, gpsTime: Option[Long], lng: Double, lat: Double,
                      accuracy: Option[Float], bearing: Option[Float], speed: Option[Float],
                      altitude: Option[Float], pdop: Option[Float], hdop: Option[Float], vdop: Option[Float],
                      num_satellites: Option[Int],
                      line_speed: Option[Int] = None) extends GpsLngLat
case class SrcDataWifiItem(bssid: String, ssid: String, level: String, frequency: String)
case class SrcDataWifi(time: Long, wifiList: Array[SrcDataWifiItem],
                       matchGps: Option[SrcDataGps] = None, matchGpsType: Option[String] = None) extends SrcDataTypeItem


object BlkAps {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.memory", "10g")
    conf.set("spark.cores.max", "672")
    conf.set("spark.executor.memory", "40g")
    conf.set("spark.akka.frameSize", "200")
    conf.setAppName("WiFi blk pre")

/*    for (j<-27.to(28)){
     val i=f"$j%02d"
     println(i)
     val wifi_path = "/user/rd/bigdata-dp/locsdk/etl-lzo-2/wifi/2015/06/"+i+"/part-r-*"
     val save_path = "/user/rd/bigdata-dp/locsdk/blk_ap_sig/2015/06/"+i+"/"*/

    val wifi_path = "/user/xiaoju/spark/etl-lzo-2/"
    val save_path = "/user/xiaoju/spark/blk_ap_sig/"

    val sc = new SparkContext(conf)
      val rdd = sc.newAPIHadoopFile(wifi_path,
        classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text])
        .map(p => p._2.toString)
        .repartition(6000)
        .map(line => {
          val cols = line.split("\t")
          var wifi = JsonUtils.parseJson[SrcDataWifi](cols(1))
          val w = wifi.wifiList.map(x => (x.bssid, x.level))
          val l = wifi.matchGps.get
          if(l != None) {
            ((l.lng*10000).toInt, (l.lat*10000).toInt, l.speed, w)
          } else {
            null
          }
       })
      .filter(x => (x != null) && (x._3.isDefined) && (x._3.get < 10))
      .flatMap(x =>
        x._4.map{ y =>
          (x._1 + "," + x._2 + "," + y._1, y._2)
        }
       ).reduceByKey((a: String, b: String) => {
        var buffer = new StringBuilder(a)
        buffer.append(",")
        buffer.append(b)
        buffer.toString()
      }).map{ x=>
        var buffer = new StringBuilder(x._1)
        buffer.append("\t")
        buffer.append(x._2)
        buffer.toString()
      }.coalesce(1000)
/*      .groupByKey()
      .map { line =>
        line._2.mkString(line._1 + "\t", ",", "")
      }*/
      //.repartition(1000)
      .saveAsTextFile(save_path)
      //MyFileUtils.saveAllLinesToLocalText(resultRdd2.collect, "out.csv")
    //} finally {
      //rdd.take(2).foreach(println)
      //println(rdd.count())
      sc.stop()
    }
//  }
}
