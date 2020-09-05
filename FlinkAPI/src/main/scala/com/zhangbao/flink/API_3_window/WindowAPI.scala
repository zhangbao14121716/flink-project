package com.zhangbao.flink.API_3_window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Zhangbao
 * @Date: 21:17 2020/9/3
 * @Description:
 *
 */
object WindowAPI {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义为EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val listInputStream: DataStream[String] = environment.fromCollection(List("zhang,male,18", "li,female,19", "wang,male,20"))
    val dataStream: DataStream[(String, String, String, Int)] = listInputStream.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1), arr(2), arr(3).toInt)
    })
    //1.对未分组的流执行开窗，不分key
    //（1）按时间长度开窗
    dataStream
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))//事件时间滑动窗口，需要设置时间语义
      //.timeWindowAll(Time.seconds(10), Time.seconds(2))//滑动窗口
      //.timeWindowAll(Time.seconds(10))//10s滚动窗口
    //（2）按事件条数开窗
    dataStream
      .countWindowAll(100, 10)//每当某一个key的个数达到10的时候,触发计算，计算最近该key最近100个元素的内容
      //.countWindowAll(100)//100条事件的滚动窗口

    //2.对分组后的流执行开窗，相当于每个分组都独自拥有自己的窗口
    //（1）每个分组按时间长度开一个窗口
    dataStream.keyBy(_._3)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))//事件时间滑动窗口，需要设置时间语义
      //.timeWindow(Time.seconds(10), Time.seconds(2))//滑动窗口
      //.timeWindow(Time.seconds(10))//10s滚动窗口
    //（2）每个分组按事件条数开窗
    dataStream.keyBy(_._3)
      .countWindow(100,10)//每当某一个key的个数达到10的时候,触发计算，计算最近该key最近100个元素的内容
      //.countWindow(100)//100条事件的滚动窗口

    //3.WindowFounction（窗口内操作的函数）
    //（1）增量聚合函数

    //（2）全窗口函数

    //-- 执行运行环境
    environment.execute("sources_and_sinks")
  }
}
