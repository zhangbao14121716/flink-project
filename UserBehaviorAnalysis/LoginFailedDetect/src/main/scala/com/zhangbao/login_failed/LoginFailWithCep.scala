package com.zhangbao.login_failed

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @Author: Zhangbao
 * @Date: 14:05 2020/9/8
 * @Description:
 * 需求：连续登录失败统计警报--CEP法则（连续多次登录，扩展性能好）
 *
 * 处理流程：
 *  1.定义Pattern模式
 *  2.CEP种方法转化PatternStream
 *  3.PatternStream---select（自定义拣选规则）
 *
 */

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据并转换成样例类类型
    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[UserLoginEvent] = environment.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp * 1000L
      })

    // 2. 定义一个匹配模式，用来检测复杂事件序列
    //    val loginFailPattern = Pattern
    //      .begin[UserLoginEvent]("firstFail").where(_.eventType == "fail")
    //      .next("secondFail").where(_.eventType == "fail")
    //      .next("thirdFail").where(_.eventType == "fail")
    //      .within(Time.seconds(5))

    //宽松近邻
    val loginFailPattern: Pattern[UserLoginEvent, UserLoginEvent] = Pattern
      .begin[UserLoginEvent]("fail")
      .where(_.eventType == "fail")
      .times(3).consecutive()
      .within(Time.seconds(5))


    // 3. 对数据流应用定义好的模式，得到PatternStream
    val patternStream: PatternStream[UserLoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 4. 检出符合匹配条件的事件序列，做转换输出
    val loginFailDataStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailSelect())

    // 5. 打印输出
    loginFailDataStream.print()

    environment.execute("login fail with cep job")
  }
}

// 实现自定义的PatternSelectFunction
class LoginFailSelect() extends PatternSelectFunction[UserLoginEvent, LoginFailWarning] {
  override def select(pattern: util.Map[String, util.List[UserLoginEvent]]): LoginFailWarning = {
    // 从map结构中可以拿到第一次和第二次登录失败的事件
    //    val firstFailEvent = pattern.get("firstFail").get(0)
    //    val secondFailEvent = pattern.get("secondFail").get(0)
    //    val thirdFailEvent = pattern.get("thirdFail").get(0)
    //    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")

    val firstFailEvent: UserLoginEvent = pattern.get("fail").get(0)
    val thirdFailEvent: UserLoginEvent = pattern.get("fail").get(2)
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp,  "login fail 3 times in 5s",thirdFailEvent.timestamp)

  }
}