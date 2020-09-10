package com.zhangbao

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 15:33 2020/9/10
 * @Description:
 * 需求：用户日活统计
 *
 * 处理逻辑：
 * 1、过滤，开全窗口、聚合
 *  （1）set集合去重
 *  （2）预聚合（去重），全窗口输出
 */

//定义输出样例类
case class UvCount(windowEnd:Long, count:Long)

object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    //创建流处理环境，配置环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(8)//设置为8时知识输出并行度为8，内部都进入一个slot
    //读取数据
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputSteam: DataStream[String] = environment.readTextFile(resource.getPath)

    //数据结构转换，提取时间戳，设置watermark
    val dataStream: DataStream[UserBehavior] = inputSteam.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //过滤，开全窗口，聚合
    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      //      .apply( new UvCountResult() ) //直接用Set集合中去重（实际上是每来一条数据都会创建一个新的集合，使用全窗口聚合）
      .aggregate(new UvCountAgg(), new UvCountAggResult()) //先预聚合（实际上是每来一条数据都会创建新的集合，使用了预聚合+全窗口聚合）

    resultStream.print("--")

    environment.execute("uv job")
  }
}
//1. 实现自定义的WindowFunction
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个set集合类型，来保存所有的userId，自动去重
    var idSet: Set[Long] = Set[Long]()
    for( userBehavior <- input )
      idSet += userBehavior.userId
    // 输出set的大小
    out.collect( UvCount(window.getEnd, idSet.size) )
  }
}


//自定义预聚合函数
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long]{
  //创建初始化累加器
  override def createAccumulator(): Set[Long] = Set[Long]()
  //每来一条数据，累加聚合值
  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId//每次相加都会创建一个新的Set集合，而Set.add()不会创建新的集合
  //返回值
  override def getResult(accumulator: Set[Long]): Long = accumulator.size
  //累加器聚合
  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

//自定义全窗口聚合函数，自定义Map实现去重
class UvCountAggResult() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  //实现apply
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    //收集数据
    out.collect(UvCount(window.getEnd,input.head))
  }
}