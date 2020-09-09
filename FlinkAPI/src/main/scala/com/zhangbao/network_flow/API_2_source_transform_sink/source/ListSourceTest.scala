package com.zhangbao.network_flow.API_2_source_transform_sink.source

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 22:51 2020/9/4
 * @Description: source
 * 从集合中读取数据流
 */
object ListSourceTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val inputStream: DataStream[String] = environment.fromCollection(List("aaa_111_AAA", "bbb_222_BBB", "ccc_333_CCC"))

//    val inputStream: DataStream[SensorReading] = environment.fromCollection(List(
//      SensorReading("sensor_1", 1547718199, 35.8),
//      SensorReading("sensor_6", 1547718201, 15.4),
//      SensorReading("sensor_7", 1547718202, 6.7),
//      SensorReading("sensor_10", 1547718205, 38.1)
//    ))
    inputStream.print("list")

    environment.execute("List")
  }
}
