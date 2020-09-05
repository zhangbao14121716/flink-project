package com.zhangbao.flink.otherapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.swing.Action.Trigger

/**
 * @Author: Zhangbao
 * @Date: 22:04 2020/9/3
 * @Description:
 *  其他重要的API--基于，窗口化的流
 *  trigger-windowedStream
 *  evictor-windowedStream
 *  allowedLateness-windowedStream
 *  sideOutputLateData-windowedStream
 *  getSideOutput-DataStream/keyedStream
 */
object OtherAPI {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义为EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val listInputStream: DataStream[String] = environment.fromCollection(List("zhang,male,18", "li,female,19", "wang,male,20"))
    val dataStream: DataStream[(String, String, String, Int)] = listInputStream.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1), arr(2), arr(3).toInt)
    })

    dataStream.keyBy(_._3)
      .countWindow(10, 2)
        //.trigger()
        //.evictor()
        //.allowedLateness(Time.seconds(60))
        //.sideOutputLateData()

    //-- 执行运行环境
    environment.execute("otherAPI")
  }
}
