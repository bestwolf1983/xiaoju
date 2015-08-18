package com.xiaoju

import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by cenyuhai on 2015/6/23.
 */
class KafkaProducer(outputTopic: String, brokerList: String) extends Serializable {
  val props = new Properties()
  props.put("metadata.broker.list", brokerList)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  @transient val config = new ProducerConfig(props)
  @transient val producer = new Producer[String, String](config)

  def send(message: String): Unit = {
    val data = new KeyedMessage[String, String](outputTopic, "", message);
    producer.send(data)
  }

  def close(): Unit = {
    producer.close()
  }


}
