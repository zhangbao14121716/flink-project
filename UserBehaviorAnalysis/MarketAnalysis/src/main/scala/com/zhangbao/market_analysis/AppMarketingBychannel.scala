package com.zhangbao.market_analysis

import java.util.UUID

import org.apache.flink.api.java.tuple.{Tuple,Tuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author: Zhangbao
 * @Date: 9:07 2020/9/8
 * @Description:
 * 需求：自定义数据源（用户，支付方式，行为）
 * 统计所有窗口安装了的不同支付方式和行为的用户总数
 *
 * 处理步骤：
 * 1、过滤、分组、开窗、处理
 *  实现ProcessWindowFunction中的process方法
 */
//

//输入数据和输出统计样例类
case class MarketingUserBehavior(userId: String, behavior:String, channel: String ,timpstamp: Long)
case class MarketingViewCount(channel:String,behavior: String, windowStart:Long ,windowEnd: Long, count:Long)

object AppMarketingBychannel {

  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //环境配置
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    //自定义数据源读取数据，提取时间戳，设置watermark
    val dataStream: DataStream[MarketingUserBehavior] = environment
      .addSource(new MySource)
      .assignAscendingTimestamps(_.timpstamp)

    //过滤、分组、开窗、处理
    val resultStream: DataStream[MarketingViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy("channel", "behavior")
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MyProcessWindowFunction)

    resultStream.print("--")
    environment.execute("job")
  }


}


//自定义数据源，实现RichSourceFunction
class MySource extends RichSourceFunction[MarketingUserBehavior]{
  //
  var running: Boolean = true
  //
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "UNINSTALL")
  val channelSet: Seq[String] = Seq("AppStore", "wechat")
  //重写run()方法
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //
    val maxElements: Long = Long.MaxValue
    var count: Long = 0L
    //
    while (running && count < maxElements) {
      //
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorSet(Random.nextInt(behaviorSet.size))
      val channel: String = channelSet(Random.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      //
      count += 1
      //
      ctx.collect(MarketingUserBehavior(id, behavior, channel,ts))
      Thread.sleep(1000)
    }
  }
  //
  override def cancel(): Unit = {
    running = false
  }
}

//自定义ProcessWindowFunction，实现process方法
class MyProcessWindowFunction() extends ProcessWindowFunction[MarketingUserBehavior, MarketingViewCount, Tuple, TimeWindow] {
  //
  override def process(key: Tuple, context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingViewCount]): Unit = {
    //
    val tuple: Tuple2[String, String] = key.asInstanceOf[Tuple2[String, String]]
    val channel: String = tuple.f0
    val behavior: String = tuple.f1
    //
    out.collect(MarketingViewCount(channel, behavior, context.window.getStart, context.window.getEnd,elements.size))
  }
}