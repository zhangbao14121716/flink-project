package com.zhangbao.login_failed

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * @Author: Zhangbao
 * @Date: 14:05 2020/9/8
 * @Description:
 * 需求：连续登录失败统计警报--升级版（如果登录数据来到的是乱序, 不用到2s结束后触发警报信息，2秒内如果达到指定次数立马发出警报）
 * -局限性：扩展性低，来纳许两次登录失败逻辑较简单，次数越多逻辑越复杂
 * 处理流程：
 * 1、分组处理
 *  实现ProcessFunction
 *  状态：如果接收到一次登录失败后判断之前状态集合中是否有失败数据。如果有，判断两者时间差是否在2s内（<=2s输出警告信息，>2不作任何处理）
 *    不管报不报警，直接清空状态，将最近一次失败数据添加进去。如果没有之前状态没有失败数据，作为该阶段第一条失败数据加入集合。
 *    如果接收到成功时清空状态集合。
 *
 */
object LoginFailPro {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型
    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[UserLoginEvent] = environment.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      //提取时间戳，设置水位线
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp * 1000L
      })

    // 分组，处理--用ProcessFunction实现连续登录失败的检测
    val resultStream: DataStream[LoginFailWarning] = loginEventStream
      .keyBy(_.userId) // 基于userId分组
      .process(new LoginFailDetectProWarning())

    resultStream.print("--")
    environment.execute("login fail pro job")
  }
}

class LoginFailDetectProWarning() extends KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]{
  // 定义状态，保存所有登录失败事件的列表
  lazy val loginFailEventListState: ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("fail-list", classOf[UserLoginEvent]))

  override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前数据是登录成功还是失败
    if( value.eventType == "fail" ){
      // 1. 如果是失败，判断之前是否已有登录失败事件
      val iter: util.Iterator[UserLoginEvent] = loginFailEventListState.get().iterator()
      if( iter.hasNext ){
        // 1.1 如果已有登录失败，继续判断是否在2秒之内
        val firstFailEvent: UserLoginEvent = iter.next()
        if( value.timestamp - firstFailEvent.timestamp <= 2 ){
          // 在2秒之内，输出报警
          out.collect( LoginFailWarning(value.userId, firstFailEvent.timestamp, "login fail in 2s for 2 times",value.timestamp))
        }
        // 不管报不报警，直接清空状态，将最近一次失败数据添加进去
        loginFailEventListState.clear()
        loginFailEventListState.add(value)
      } else {
        // 1.2 如果没有数据，当前是第一次登录失败，直接添加到状态列表
        loginFailEventListState.add(value)
      }
    } else {
      // 2. 如果是成功，清空状态，重新开始
      loginFailEventListState.clear()
    }
  }
}