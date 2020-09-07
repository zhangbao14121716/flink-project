package com.zhangbao.flink.API_3_window_watermark_otherAPI

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Author: Zhangbao
 * @Date: 21:17 2020/9/3
 * @Description:
 * 1.时间语义
 * EventTime
 * ProcessingTime
 * IngestionTime
 * 2.Watermark
 *  2.1
 * 3.开窗函数（DataStream和KeyedStream中的方法）
 *  3.1 TimeWindow
 *  3.2 eventWindow
 *  3.3 window
 * 4.窗口函数（）
 *  4.1 增量聚合函数
 * reduce(自定义ReduceFunction)
 * aggregate(自定义AggregateFunction)
 *  4.2 全窗口函数
 * 5.其他API
 *  5.1 Trigger
 *  5.2 evictor
 *  5.2
 *
 */
object WindowAPITest_1 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //------------------------------------------------------------
    //设置时间语义为EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    //------------------------------------------------------------
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val inputStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim().toDouble)
    })

    //设置时间戳和水印()
    val assigerWatermarkStream: DataStream[SensorReading] = dataStream
      //.assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })
    //------------------------------------------------------------
    //1.对未分组的流执行开窗，不分key
    //（1）按时间长度开窗
    dataStream
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))) //事件时间滑动窗口，需要设置时间语义
      //.timeWindowAll(Time.seconds(10), Time.seconds(2))//滑动窗口
      //.timeWindowAll(Time.seconds(10))//10s滚动窗口
    //（2）按事件条数开窗
    dataStream
      .countWindowAll(100, 10) //每当某一个key的个数达到10的时候,触发计算，计算最近该key最近100个元素的内容
    //.countWindowAll(100)//100条事件的滚动窗口

    //2.对分组后的流执行开窗，相当于每个分组都独自拥有自己的窗口
    //（1）每个分组按时间长度开一个窗口
    val windowedStream: WindowedStream[SensorReading, String, TimeWindow] = assigerWatermarkStream
      .keyBy(_.id)//
      //.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))) //事件时间滑动窗口，需要设置时间语义
      //.timeWindow(Time.seconds(10), Time.seconds(2))//滑动窗口
      .timeWindow(Time.seconds(10))//10s滚动窗口
    //（2）每个分组按事件条数开窗
    dataStream
      .keyBy(_.id)
      .countWindow(100, 10) //每当某一个key的个数达到10的时候,触发计算，计算最近该key最近100个元素的内容
    //.countWindow(100)//100条事件的滚动窗口

    //3.WindowFounction（窗口内操作的函数）
    //（1）增量聚合函数
    windowedStream
      //.reduce(new MyMaxTemp())
      .aggregate(new MayAvgTemp())
    //（2）全窗口函数

    //-----------------------------------------------------------------------
    // 其他API
    val aggStream: DataStream[SensorReading] = windowedStream
      //.trigger()
      //.evictor()
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late_time>1min"))//侧输出流
      .reduce(new MyMaxTemp)
      //获取侧输出流
    aggStream.getSideOutput(new OutputTag[SensorReading]("late_time>1min"))
    //-- 执行运行环境
    environment.execute("window_watermark")
  }
}

//1.自定义WindowedStream后reduce方法中的ReduceFunction
//自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}

//2.自定义WindowedStream后aggregate方法中的AggregateFunction
//自定义一个求平均温度的聚合函数
class MayAvgTemp() extends AggregateFunction[SensorReading, (String, Double, Int), (String, Double)] {
  //创建，初始化累加器
  override def createAccumulator(): (String, Double, Int) = {
    ("", 0.0, 0)
  }
  //累加器累积值
  override def add(value: SensorReading, accumulator: (String, Double, Int)): (String, Double, Int) = {
    (value.id, accumulator._2 + value.temperature, accumulator._3 + 1)
  }
  //求平均值结果输出
  override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
    (accumulator._1, accumulator._2 / accumulator._3)
  }
  //累加器之间聚合
  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
    (a._1 + b._1, a._2 + b._2, a._3 + b._3)
  }
}



