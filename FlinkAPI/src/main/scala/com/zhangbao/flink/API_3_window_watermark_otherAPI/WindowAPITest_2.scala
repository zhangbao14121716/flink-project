package com.zhangbao.flink.API_3_window_watermark_otherAPI

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Zhangbao
 * @Date: 9:42 2020/9/1
 * @Description:
 * 窗口API演示，防止数据丢失三道防线
 * 1.Watermark（基于DataStream）
 * 2.allowedLateness(允许最大等待时间),（基于WindowedStream）
 * 3.侧输出流的设置（基于WindowedStream）和获取（基于DataStream）
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(100)
    environment.setParallelism(1)

    //读取数据
    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
    //设置时间戳，和水印
      //.assignAscendingTimestamps( data => data.timestamp * 1000L )    // 升序数据的时间戳提取
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    //开窗聚合操作
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
      //窗口分配器
      .timeWindow(Time.seconds(10)) //10秒大小的滚动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(1)))//1s的会话窗口
      //.window( TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(10)) )//带10分钟偏移量的1小时滚动窗口
      //.window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)) )//1小时窗口，10分钟滑动一次
      //.countWindow( 10, 2 )//滑动计数窗口

      //可选API
      //.trigger()
      //.evictor()
      .allowedLateness(Time.minutes(1))//延迟时间
      .sideOutputLateData(new OutputTag[SensorReading]("late-data"))//侧输出流

      // 窗口函数
      //        .minBy("temperature")
      //        .reduce( (curState, newData) => SensorReading(newData.id, newData.timestamp + 1, curState.temperature.max(newData.temperature))
      .reduce(new MyMaxTemp())

    aggStream.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")

    dataStream.print("data")
    aggStream.print("agg")

    environment.execute("window api job")
  }
}


