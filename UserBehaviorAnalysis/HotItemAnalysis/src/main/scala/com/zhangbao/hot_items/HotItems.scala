package com.zhangbao.hot_items

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 13:52 2020/9/5
 * @Description:
 * 需求：热门商品topN
 * 主要逻辑：
 * 过滤、分组、开窗、聚合
 * 分组、处理
 * 自定义AggregateFunction<IN, ACC, OUT>
 *  accumulator
 * 自定义WindowFunction[IN, OUT, KEY, W <: Window]
 *  apply
 * 自定义KeyedProcessFunction<K, I, O>
 *  状态，定时器
 */
//定义用户行为样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义聚合结果样例类
case class ItemCountResult(itemId: Long, count: Long, windowEnd: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    //从source中得到文件url
    val urlPath: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //结构转换,并设置时间戳，默认的水位线+1ms
    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //
    //dataStream.print("data")
    //过滤、分组、开窗、聚合
    val aggDataStream: DataStream[ItemCountResult] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new ItemsCountAggFunction(), new ItemWindowFunction())
    //
    //aggDataStream.print("agg")

    //分组，处理
    val resultStream: DataStream[String] = aggDataStream
      .keyBy("windowEnd")
      .process(new ItemIdResultKeyedProcessFunction(5))

    resultStream.print("result")

    environment.execute("com.zhangbao.hot_items.HotItems-job")
  }
}

//自定义预聚合函数
class ItemsCountAggFunction() extends AggregateFunction[UserBehavior, Long, Long] {
  //创建acc，并初始化
  override def createAccumulator(): Long = 0L
  //窗口内每来一条数据，acc+1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1
  //返回acc的值
  override def getResult(accumulator: Long): Long = accumulator
  //两和acc聚合
  override def merge(a: Long, b: Long): Long = a + b
}

//自定义全窗口函数
class ItemWindowFunction() extends WindowFunction[Long, ItemCountResult, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemCountResult]): Unit = {
    //从Java元组中获取key
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    //从输入的数据中与聚合结果的状态
    val head: Long = input.head
    //从Window中获取窗口信息（窗口结束时间）
    val windowEnd: Long = window.getEnd
    //收集聚合后的结果样例类
    out.collect(ItemCountResult(itemId, head, windowEnd))
  }
}

//自定以ProcessFunction处理结果数据
class ItemIdResultKeyedProcessFunction(n: Int) extends KeyedProcessFunction[Tuple, ItemCountResult, String] {
  //状态编程,将一个窗口的每个key的ItemCount状态存入List集合里面（每个窗口维护一个集合用于存放该窗口的不同key的信息）
  lazy private val lastItemCountResultState: ListState[ItemCountResult] = getRuntimeContext.getListState(new ListStateDescriptor[ItemCountResult]("com.zhangbao.hot_items.ItemCountResult-State", classOf[ItemCountResult]))

  //处理函数，每一个ItemCountResult元素
  override def processElement(value: ItemCountResult, ctx: KeyedProcessFunction[Tuple, ItemCountResult, String]#Context, out: Collector[String]): Unit = {
    //没来一个元素的数据将状态添加到List集合
    lastItemCountResultState.add(value)
    //设置定时器，定时具体操作放在onTimer中执行
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  //时间语义下的EventTime大于WindowEnd+100，都会触发一次该窗口的定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemCountResult, String]#OnTimerContext, out: Collector[String]): Unit = {
    //引入隐式转换将java的iterable转换成List
    import scala.collection.JavaConverters._
    //获取一个窗口的所有状态
    val allItemCountResult: List[ItemCountResult] = lastItemCountResultState.get().asScala.toList
    //提前清除状态
    lastItemCountResultState.clear()
    //排序取topN
    val resultList: List[ItemCountResult] = allItemCountResult.sortBy(_.count)(Ordering.Long.reverse)
      .take(n)
    //结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间: ").append(ctx.getCurrentKey).append("\n")
    //遍历数组展示所有信息
    for (i <- resultList.indices) {
      val currentItemViewCount: ItemCountResult = resultList(i)
      result.append("NO.").append(i + 1).append(":")
        .append("\t 商品 ID = ").append(currentItemViewCount.itemId)
        .append("\t 热门度 = ").append(currentItemViewCount.count)
        .append("\n")
    }
    result.append("\n================================\n")

    //控制输出频率
    Thread.sleep(100)
    out.collect(result.toString())
  }
}