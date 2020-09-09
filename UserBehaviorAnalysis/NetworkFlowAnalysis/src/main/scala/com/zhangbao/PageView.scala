package com.zhangbao

import java.net.URL

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author: Zhangbao
 * @Date: 7:17 2020/9/9
 * @Description:
 * 需求：统计浏览页面总人数
 */
// 定义输入输出样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(8)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = environment.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
    // 开窗统计
    val pvCountStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
      //      .map( data => ("pv", 1L) )
      //      .keyBy(_._1)    // 指定一个dummy key，所有数据都分到一组
      .map(new MyMapper()) // 自定义map函数，随机生成key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountResult())

    // 把窗口内所有分组数据汇总起来
    val pvTotalStream: DataStream[String] = pvCountStream
      .keyBy(_.windowEnd) // 按窗口结束时间分组
      //      .sum("count")
      .process(new TotalPvCountResult())

    pvTotalStream.print()

    environment.execute("pv to 8 core print")
  }
}

//
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  //
  override def map(value: UserBehavior): (String, Long) = {
    //
    (Random.nextString(10), 1L)
  }
}
//
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  //
  override def createAccumulator(): Long = 0L
  //
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1
  //
  override def getResult(accumulator: Long): Long = accumulator
  //
  override def merge(a: Long, b: Long): Long = a + b
}

//
class PvCountResult() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  //
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    //
    out.collect(PvCount(window.getEnd, input.head))
  }
}
//状态编程
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, String]{
  //
  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, String]#Context, out: Collector[String]): Unit = {

  }
}