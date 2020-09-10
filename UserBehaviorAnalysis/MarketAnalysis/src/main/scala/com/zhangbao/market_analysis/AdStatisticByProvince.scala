package com.zhangbao.market_analysis

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 10:19 2020/9/8
 * @Description:
 * 需求：广告点击上限统计及预警
 * 如果统计上限加入黑名单流输出
 *
 * 处理思路：
 * 1、先分组，处理，侧流输出上限的黑名单。主流输出要统计的用户。
 *  process：
 *  状态：点击量，是否已经添加进入黑名单，每天0点
 *  定时器：每天0点触发
 * 2、过滤，分组，开窗，聚合
 */
//定义输入和主流输出样例类
case class AdClickLog(usesrId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class AdViewCountByProvince(WindowEnd: Long, province: String, count: Long)
//测输出流输出警报信息
case class BlackWarningInfo(userId:Long, adId:Long, warningInfo:String)

object AdStatisticByProvince {

  def main(args: Array[String]): Unit = {
    //创建，配置流执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //定义时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据流
    val urlPath: URL = getClass.getResource("/AdClickLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //结构转换，提取时间戳和设置watermark
    val dataStream: DataStream[AdClickLog] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      AdClickLog(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim, arr(3).trim, arr(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    //分组（两个字段），处理（）
    val filterStream: DataStream[AdClickLog] = dataStream
      .keyBy(data => (data.usesrId, data.adId)) //按用户Id和广告Id分组
      .process(new MyFilterProcessFunction(100)) //自定义过滤过程将超出点击上限的用户数据输出到侧输出流

    //过滤，分组，开窗，聚合
    val resultStream: DataStream[AdViewCountByProvince] = filterStream
      .filter(_.province != null)
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new MyAggFunction, new MyWindowFunction)

    //主流输出按省份开窗聚合的点击总数
    resultStream.print("--")
    //侧输出流输出黑名单用户数据
    filterStream.getSideOutput(new OutputTag[BlackWarningInfo]("black-list")).print("==")

    environment.execute("--")
  }
}

//自定义窗口的预聚合函数
class MyAggFunction extends AggregateFunction[AdClickLog, Long, Long] {
  //创建初始化累加器
  override def createAccumulator(): Long = 0L
  //每来一条数据，对每个窗口聚合每个key（省份）的点击总数
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1
  //返回结果当前每个key（省份）的的点击总数
  override def getResult(accumulator: Long): Long = accumulator
  //当前
  override def merge(a: Long, b: Long): Long = a + b
}

//自定义全窗口函数，用于将当前窗口聚合值按需求结构化输出
class MyWindowFunction extends  WindowFunction[Long, AdViewCountByProvince, String, TimeWindow]{
  //实现apply方法
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {
    //收集数据，输出
    out.collect(AdViewCountByProvince(window.getEnd, key, input.head))
  }
}

//自定义ProcessFunction，进行状态编程，和定时器处理
class MyFilterProcessFunction(maxClick: Long) extends KeyedProcessFunction[(Long,Long), AdClickLog, AdClickLog] {
  //每个key(用户,广告Id)点击量聚合状态
  lazy private val lastCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastcount", classOf[Long]))
  //0点清空各种状态的时间戳，每天一个状态
  lazy private val resetState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset", classOf[Long]))
  //是否已经添加进入黑名单状态
  lazy private val isWarningAndBlack: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("last", classOf[Boolean]))
  //重写方法ProcessElement方法
  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //获取当天点击量
    val currentCount: Long = lastCountState.value()
    //判断是否是当天的第一个数据，如果是，注册第二天0点的定时器
    if (currentCount == 0) {
      val newTimestamp: Long = ((value.timestamp / 1000 / 60 / 60 / 24 + 1) * 24 - 8) * 60 * 60 * 1000
      ctx.timerService().registerProcessingTimeTimer(newTimestamp)
      resetState.update(newTimestamp)
    }
    // 过滤过程：判断是否达到了上限，如果达到，那么加入黑名单侧输出流输出
    if(currentCount >= maxClick){
      //如果之前没有添加到黑名单，否则不做任何措施直接return
      if(!isWarningAndBlack.value()) {
        isWarningAndBlack.update(true)
        ctx.output(new OutputTag[BlackWarningInfo]("black-list"),
          BlackWarningInfo(value.usesrId, value.adId, s"black user: $maxClick times today"))
      }
      return
    }
    //主流输出点击量信息
    out.collect(value)
    //每执行一次方法，一定来一条数据所以要对监控状态更新
    lastCountState.update(currentCount + 1)
  }
  //第二天00:00清空所有状态，重新统计
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //判断触发的定时器是否是00:00的定时器，如果是清空前一天所有状态
    if(timestamp == resetState.value()) {
      lastCountState.clear()
      isWarningAndBlack.clear()
      resetState.clear()
    }
  }
}

