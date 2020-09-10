package com.zhangbao

import java.net.URL

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
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
 * 需求：统计浏览页面总人数：为防止数据倾斜，将数据打散重新给定key，进行count统计
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

//自定义map实现自定义map
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  //重写map方法
  override def map(value: UserBehavior): (String, Long) = {
    //为每条数据重新分配随机key，（key，1L）
    (Random.nextString(10), 1L)
  }
}
//窗口预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  //创建初始化累加器
  override def createAccumulator(): Long = 0L
  //窗口每来一条数据，查找对应key累加数据
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1
  //返回值
  override def getResult(accumulator: Long): Long = accumulator
  //累加器聚合
  override def merge(a: Long, b: Long): Long = a + b
}

//全窗口函数输出样例类
class PvCountResult() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  //实现apply方法
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    //收集窗口聚合结果
    out.collect(PvCount(window.getEnd, input.head))
  }
}
//状态编程，
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, String]{
  //当前窗口聚合值状态
  lazy private val currentTotalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-count-current-window", classOf[Long]))
  //实现processElement方法
  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, String]#Context, out: Collector[String]): Unit = {
    //获取总的聚合值
    val currentTotalCount: Long = currentTotalCountState.value()
    //状态叠加
    currentTotalCountState.update(currentTotalCount + value.count)
    //窗口结束定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }
  //定时器触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发时的操作
    out.collect(s"WidowEnd--> $timestamp, ${currentTotalCountState.value()}")
    //状态清空
    currentTotalCountState.clear()
  }
}