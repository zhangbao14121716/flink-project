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
 *
 */

//
case class UserLoginEvent(userId: Long, ip: String, loginInfo: String, timestamp: Long)

//
case class LoginFailWarning(userId:Long, ip:String, loginInfo:String,timestamp:Long)

object LoginFail {

  def main(args: Array[String]): Unit = {
    //
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //
    environment.setParallelism(1)
    //
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //
    val urlPath: URL = getClass.getResource("/LoginLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //
    val dataStream: DataStream[UserLoginEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream: DataStream[LoginFailWarning] = dataStream.keyBy(_.userId)
      .process(new MyLoginProcessFunction(2))

    resultStream.print()

    environment.execute("login fail job")
  }
}

//
class MyLoginProcessFunction(n: Int) extends KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]{
  //
  lazy private val loginFailedListState: ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("last-login-failed", classOf[UserLoginEvent]))

  //
  lazy private val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-timer", classOf[Long]))
  //
  override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //
    if (value.loginInfo == "fail") {
      //
      loginFailedListState.add(value)
      //
      val ts: Long = value.timestamp * 1000L + 2000L
      //
      ctx.timerService().registerEventTimeTimer(ts)
      //
      timerState.update(ts)
    }else {
      //
      ctx.timerService().deleteEventTimeTimer(timerState.value())
      //
      loginFailedListState.clear()
      //
      timerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit ={
    //
    import scala.collection.JavaConverters._
    if (loginFailedListState.get().asScala.toList.size >= n) {
      val head: UserLoginEvent = loginFailedListState.get().asScala.toList.head
      out.collect(LoginFailWarning(head.userId,head.ip,s"warning! user: ${head.userId} login failed in 2 s",head.timestamp))

    }
  }

}
