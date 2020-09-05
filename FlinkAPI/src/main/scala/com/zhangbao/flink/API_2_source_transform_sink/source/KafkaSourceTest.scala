package com.zhangbao.flink.API_2_source_transform_sink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @Author: Zhangbao
 * @Date: 0:49 2020/9/5
 * @Description:
 *  使用kafka的source连接器
 */
object KafkaSourceTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //kafka配置信息
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop103:9092,hadoop102:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //
    val inputKafkaStream: DataStream[String] = environment
      .addSource(new FlinkKafkaConsumer011[String]("Sensor", new SimpleStringSchema(), properties))

    environment.execute("kafka-source")
  }
}
