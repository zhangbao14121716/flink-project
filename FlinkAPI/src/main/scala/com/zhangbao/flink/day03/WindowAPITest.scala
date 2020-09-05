package com.zhangbao.flink.day03

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Author: Zhangbao
 * @Date: 9:42 2020/9/1
 * @Description:
 *
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(100)
    environment.setParallelism(1)

    // 读取数据
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    //    val inputStream: DataStream[String] = env.readTextFile(filePath)
    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

      //      .assignAscendingTimestamps( data => data.timestamp * 1000L )    // 升序数据的时间戳提取
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 开窗聚合操作
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
      // 窗口分配器
      .timeWindow(Time.seconds(10)) // 10秒大小的滚动窗口
      //        .window( EventTimeSessionWindows.withGap(Time.seconds(1)) )    // 会话窗口
      //        .window( TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 带10分钟偏移量的1小时滚动窗口
      //        .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 1小时窗口，10分钟滑动一次
      //        .countWindow( 10, 2 )    // 滑动计数窗口

      // 可选API
      //        .trigger()
      //        .evictor()
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late-data"))

      // 窗口函数
      //      .minBy("temperature")
      //      .reduce( (curState, newData) => SensorReading(newData.id, newData.timestamp + 1, curState.temperature.max(newData.temperature))

      .reduce(new MyMaxTemp())

    aggStream.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")

    dataStream.print("data")
    aggStream.print("agg")

    environment.execute("window api job")
  }
}

// 自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}

// 自定义一个求平均温度的聚合函数
class MyAvgTemp() extends AggregateFunction[SensorReading, (String, Double, Int), (String, Double)]{
  override def add(value: SensorReading, accumulator: (String, Double, Int)): (String, Double, Int) =
    ( value.id, accumulator._2 + value.temperature, accumulator._3 + 1 )

  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

  override def getResult(accumulator: (String, Double, Int)): (String, Double) =
    ( accumulator._1, accumulator._2 / accumulator._3 )

  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) =
    ( a._1, a._2 + b._2, a._3 + b._3 )
}
