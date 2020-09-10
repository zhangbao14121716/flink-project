package com.zhangbao.hot_items

import java.net.URL
import java.util.Properties

import scala.io.{BufferedSource, Source}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
 * @Author: Zhangbao
 * @Date: 17:52 2020/9/9
 * @Description:
 *  kafka生产者工具类：用于提取本地文件中测试数据向kafka中生产数据
 *
 */

object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    // kafka配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // 从文件读取数据
    val urlPath: URL = getClass.getResource("/UserBehavior.csv")
    val bufferedSource: BufferedSource = Source.fromFile(urlPath.getPath)
    for( line <- bufferedSource.getLines() ){
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
