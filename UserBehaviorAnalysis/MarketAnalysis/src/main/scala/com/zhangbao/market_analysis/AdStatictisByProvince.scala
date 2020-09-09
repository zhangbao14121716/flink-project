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
 *
 */
case class AdClickLog(usesrId: Long, adId: Long, province: String, city: String, timestamp: Long)

//
case class AdViewCountByProvince(WindowEnd: Long, province: String, count: Long)


case class BlackWarningInfo(userId:Long, adId:Long, warningInfo:String)

object AdStatictisByProvince {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //
    environment.setParallelism(1)
    //
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //
    val urlPath: URL = getClass.getResource("/AdClickLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)

    val dataStream: DataStream[AdClickLog] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      AdClickLog(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim, arr(3).trim, arr(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    //
    val filterStream: DataStream[AdClickLog] = dataStream
      .keyBy(data => (data.usesrId, data.adId))
      .process(new MyFilterProcessFunction(100))



    //
    val resultStream: DataStream[AdViewCountByProvince] = filterStream
      .filter(_.province != null)
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new MyAggFunction, new MyWindowFunction)

    resultStream.print("--")
    filterStream.getSideOutput(new OutputTag[BlackWarningInfo]("black-list")).print("==")

    environment.execute("--")
  }
}

//
class MyAggFunction extends AggregateFunction[AdClickLog, Long, Long] {
  //
  override def createAccumulator(): Long = 0L
  //
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1
  //
  override def getResult(accumulator: Long): Long = accumulator
  //
  override def merge(a: Long, b: Long): Long = a + b
}

//
class MyWindowFunction extends  WindowFunction[Long, AdViewCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {
    out.collect(AdViewCountByProvince(window.getEnd, key, input.head))
  }
}


class MyFilterProcessFunction(maxClick: Long) extends KeyedProcessFunction[(Long,Long), AdClickLog, AdClickLog] {
  //点击量
  lazy private val lastCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastcount", classOf[Long]))
  //清空各种状态的时间戳
  lazy private val resetState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset", classOf[Long]))
  //是否已经添加进入黑名单
  lazy private val isWarningAndBlack: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("last", classOf[Boolean]))
  //
  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //
    val currentCount: Long = lastCountState.value()
    //
    if (currentCount == 0) {
      //
      val newTimestamp: Long = ((value.timestamp / 1000 / 60 / 60 / 24 + 1) * 24 - 8) * 60 * 60 * 1000
      //
      ctx.timerService().registerProcessingTimeTimer(newTimestamp)
      //
      resetState.update(newTimestamp)
    }

    //
    if(currentCount >= maxClick){
      //
      if(!isWarningAndBlack.value()) {
        isWarningAndBlack.update(true)
        ctx.output(new OutputTag[BlackWarningInfo]("black-list"),
          BlackWarningInfo(value.usesrId, value.adId, s"black user: $maxClick times today"))
      }
      return
    }
    //
    out.collect(value)
    //
    lastCountState.update(currentCount + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //
    lastCountState.clear()
    //
    isWarningAndBlack.clear()
    //
    resetState.clear()
  }
}

