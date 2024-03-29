/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaoju

import java.io.File

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokersDirectKafkaWordCount$
KafkaWordCount.scala
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port
 *    topic1,topic2
 *
 *    hdp6.jx:9092  test
 *    kafka
 *    ~/kafka_2.10-0.8.2.0
 *
 *    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
 *
 *
 */

/**
 * TODO WARN  [Executor task launch worker-0] kafka.KafkaRDD (KafkaRDD.scala:compute(89)) -
 * Beginning offset ${part.fromOffset} is the same as ending offset skipping test
 */

object DirectKafkaWordCount {

  def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String)
  : StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val counts = "Counts at time " + time + " " + rdd.collect().mkString("[", ", ", "]")
      println(counts)
      println("Appending to " + outputFile.getAbsolutePath)
    })
    ssc
  }

  def main(args: Array[String]) {

    // Create context with 2 second batch interval"hdp1.jx:2181,hdp2.jx:2181,hdp3.jx:2181"
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[4]")
    val ssc =  new StreamingContext(sparkConf, Seconds(3))


    // Create direct kafka stream with brokers and topics
    //http://kafka.apache.org/documentation.html#quickstart     see more config
    //                                                          spark.streaming.kafka
    val topicsSet = Set("taxidriverloc")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hdp1.jx:9092,hdp2.jx:9092,hdp3.jx:9092",
 //      "auto.offset.reset" -> "smallest",   //from start
       "group.id" ->"testDirectDemo"     //group

    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print

    val lines  = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
