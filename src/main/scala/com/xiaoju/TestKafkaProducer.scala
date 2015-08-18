package com.xiaoju

import java.util.{Date, Properties}
import kafka.producer.{Producer, ProducerConfig, KeyedMessage}

/**
 * Created by Administrator on 15-4-9.
 */
object TestKafkaProducer {
  def main(args: Array[String]) = {
    // 设置配置属性
    val props = new Properties();
    props.put("metadata.broker.list","hdp1.jx:9092,hdp2.jx:9092,hdp3.jx:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    // key.serializer.class默认为serializer.class
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    // 可选配置，如果不配置，则使用默认的partitioner
    //props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
    // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
    // 值为0,1,-1,可以参考
    // http://kafka.apache.org/08/configuration.html
    props.put("request.required.acks", "1");
    val config = new ProducerConfig(props);

    // 创建producer
    val producer = new Producer[String, String](config);
    // 产生并发送消息
    val start=System.currentTimeMillis();
    while (true) {
      val runtime = new Date().getTime();
      val msg = runtime + ",www.example.com,";
      //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
      val data = new KeyedMessage[String, String](
        "testPythonStreaming", "", msg);
      producer.send(data);
      println("写入：" + msg)
    }
    System.out.println("耗时:" + (System.currentTimeMillis() - start));
    // 关闭producer
    producer.close();
  }
}
