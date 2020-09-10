package com.zhangbao.login_failed

import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 14:05 2020/9/8
 * @Description:
 * 需求：连续登录失败统计警报
 *
 * 处理流程：
 * 1、分组处理
 *  实现ProcessFunction
 *  状态：一次登录失败后两秒内失败加入集合，如果未触发定时器，但登录成功则清空定时器，和登录失败集合
 *  定时器触发：计算失败状态集合中个数
 *
 */

//输入数据样例类
case class UserLoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
//连续登录失败警报样例类
case class LoginFailWarning(userId:Long, firstFailTimestamp:Long, loginInfo:String,timestamp:Long)

object LoginFail {

  def main(args: Array[String]): Unit = {
    //创建配置流执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取数据
    val urlPath: URL = getClass.getResource("/LoginLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //数据结构转换，提取时间戳，设置watermark
    val dataStream: DataStream[UserLoginEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //分组，处理
    val resultStream: DataStream[LoginFailWarning] = dataStream
      .keyBy(_.userId)
      .process(new MyLoginProcessFunction(2))

    resultStream.print("--")
    environment.execute("login fail job")
  }
}

//自定义实现KeyedProcessFunction，状态，定时器
class MyLoginProcessFunction(n: Int) extends KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]{
  //每个key（用户）登录失败状态集合
  lazy private val loginFailedListState: ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("last-login-failed", classOf[UserLoginEvent]))
  //定时器触发的时间状态
  lazy private val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-timer", classOf[Long]))
  //实现processElement方法
  override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //对每条数据执行一次processElement方法
    if (value.eventType == "fail") {
      //如果是登录失败，添加数据到列表状态中
      loginFailedListState.add(value)

      //定义一个等待2秒后触发的定时器
      val ts: Long = value.timestamp * 1000L + 2000L
      ctx.timerService().registerEventTimeTimer(ts)
      //定时器时间状态更新到两秒后
      timerTsState.update(ts)
    }else {
      //清除要触发的定时器
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      //清除所有状态
      loginFailedListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit ={
    //如果两秒内登录失败的次数大于指定次数，输出警告信息
    import scala.collection.JavaConverters._
    if (loginFailedListState.get().asScala.toList.size >= n) {
      val head: UserLoginEvent = loginFailedListState.get().asScala.toList.head
      out.collect(LoginFailWarning(head.userId,head.timestamp,s"warning! user: ${head.userId} login failed in 2 s",ctx.getCurrentKey))
    }
  }

}
