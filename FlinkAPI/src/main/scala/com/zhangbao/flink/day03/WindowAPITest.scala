package com.zhangbao.flink.day03

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Author: Zhangbao
 * @Date: 9:42 2020/9/1
 * @Description:
 *
 */
object WindowAPITest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.socketTextStream("hadoop103", 7777)
    val windowStream: WindowedStream[SensorReading, String, TimeWindow] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    }).keyBy(_.id)
      .timeWindow(Time.seconds(15))
//    windowStream.

//    result.print()

    environment.execute()
  }
}
