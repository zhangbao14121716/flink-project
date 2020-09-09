package com.zhangbao

import java.net.URL
import java.security.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 * @Author: Zhangbao
 * @Date: 9:07 2020/9/7
 * @Description:
 * 需求：热门页面浏览流量统计TopN
 *
 */
//页面日志样例类
case class WebSeverLogEvent(ip: String, eventTime: Long, method: String, url: String)
//窗口聚合样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)


object NetworkFlow {

  def main(args: Array[String]): Unit = {
    //创建流环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置时间并行度
    environment.setParallelism(8)
    //读取数据流
    val urlPath: URL = getClass.getResource("/apache.log")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //结构转换，并设置时间戳，和watermark
    val dataStream: DataStream[WebSeverLogEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(" ")
      val timestamp: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3).trim()).getTime
      WebSeverLogEvent(arr(0).trim(), timestamp, arr(5).trim(), arr(6).trim())
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WebSeverLogEvent](Time.seconds(3)) {
          override def extractTimestamp(element: WebSeverLogEvent): Long = element.eventTime
        })
    //创建测输出流标签
    val lateTag: OutputTag[WebSeverLogEvent] = new OutputTag[WebSeverLogEvent]("late-log")
    //过滤，分组，开窗，聚合
    val aggDataStream: DataStream[PageViewCount] = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)
      .aggregate(new PageViewCountAggFunction(), new PageViewResultWindowFunction)
    //分组，处理（排序）
    val resultStream: DataStream[String] = aggDataStream
      .keyBy(_.windowEnd)
      .process(new TopNPageResultProcessFunction(5))

    resultStream.print("--")
    environment.execute("PageViewTopN")
  }
}

//自定义实现AggregateFunction
class PageViewCountAggFunction() extends AggregateFunction[WebSeverLogEvent, Long, Long]{
  //创建并初始化累加器
  override def createAccumulator(): Long = 0L
  //对每个窗口中的每个Key，每来一条数据进行相应累加
  override def add(value: WebSeverLogEvent, accumulator: Long): Long = accumulator + 1
  //获取累加器结果
  override def getResult(accumulator: Long): Long = accumulator
  //两个累加器聚合
  override def merge(a: Long, b: Long): Long = a + b
}

//自定义实现WindowFunction
class PageViewResultWindowFunction() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  //实现apply方法
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    //收集所有窗口信息
    out.collect(PageViewCount(key, window.getEnd, input.head))
  }
}

//自定义实现KeyedProcessFunction，进行排序输出
class TopNPageResultProcessFunction(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
  //为了对于同一个Key进行更新操作，定义映射状态（防止相同Key的状态重复记录，导致最后输出有多个相同Key(url)的数据输出）
  lazy private val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("late-stream", classOf[String], classOf[Long]))
  //实现proceeElement方法
  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //将（url，count）状态保存在Map中（自动对url状态去重）
    pageViewCountMapState.put(value.url, value.count)
    //注册一个定时器，windowEnd+100后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    //注册一个一分钟的定时器，用于清理状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }
  //重写定时器，在定时器中清理状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器如果到达allowedLateness的最大时间，要清空状态
    if (ctx.getCurrentKey + 60 * 1000L == timestamp) {
      pageViewCountMapState.clear()
      return
    }
    //获取pageViewCountMapState的状态
    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter: util.Iterator[util.Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry: util.Map.Entry[String, Long] = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    //状态排序取TopN
    val topNHotPageViewCounts: ListBuffer[(String, Long)] = allPageViewCounts.sortWith(_._2 > _._2).take(n)



    // 排名信息格式化打印输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(ctx.getCurrentKey).append("\n")
    // 遍历topN 列表，逐个输出
    for( i <- topNHotPageViewCounts.indices ){
      val currentItemViewCount: (String, Long) = topNHotPageViewCounts(i)
      result.append("NO.").append( i + 1 ).append(":")
        .append("\t 页面 URL = ").append( currentItemViewCount._1 )
        .append("\t 热门度 = ").append( currentItemViewCount._2 )
        .append("\n")
    }
    result.append("\n =================================== \n\n")

    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}