package com.zhangbao.network_flow.API_2_source_transform_sink.sink

import java.util.Properties

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @Author: Zhangbao
 * @Date: 0:31 2020/9/5
 * @Description:
 *  1.kafka消费数据获得数据流
 *  2.kafka生产者将获得的数据流再写入kafka
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //从Kafka消费数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop103:9092")
    properties.setProperty("group.id", "consumer-group")
    val inputStream: DataStream[String] = environment.addSource( new FlinkKafkaConsumer011[String]("source_from_sensor", new SimpleStringSchema(), properties) )

    //转换数据结构
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //向Kafka生产数据
    dataStream.addSink(new FlinkKafkaProducer011[SensorReading]("hadoop103:9092","sink_to_sensor",new SerializationSchema[SensorReading] {
      override def serialize(element: SensorReading): Array[Byte] = {
        (element.id + element.temperature.toString).toArray.map(_.toByte)
      }
    }))

    environment.execute("Kafka_to_Kafka")
  }
}
