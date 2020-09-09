package com.zhangbao.orderpay_detail

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Zhangbao
 * @Date: 9:00 2020/9/9
 * @Description:
 * 需求：下单后15分钟之内支付的订单
 */
//
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

//
case class OrderPayResult(orderId: Long, resultMsg: String)

object OrderPayTimeout {

  def main(args: Array[String]): Unit = {
    //
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //
    environment.setParallelism(1)
    //
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val urlPath: URL = getClass.getResource("/OrderLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    //
    val dataStream: DataStream[OrderEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //CEP
    //定义匹配模式
    val orderTimeoutPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where((_.eventType == "create"))
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream.keyBy(_.orderId), orderTimeoutPattern)

    //
    val resultStream: DataStream[OrderPayResult] = patternStream.select(new OrderPaySelect())

    resultStream.print("--")

    environment.execute("order pay timeout job")
  }
}

//自定义PatternSelectFunction，拣选出符合模式的流放入Map集合[拣选事件, 该事件的集合(1个元素)]=>[("create",List()),("pay",List())]
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderPayResult]{
  //实现select方法
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
    import scala.collection.JavaConverters._
    //
    val headCreateOrderEvent: OrderEvent = map.get("create").asScala.toList.head
    //
    val headPayOrderEvent: OrderEvent = map.get("pay").asScala.toList.head
    OrderPayResult(headCreateOrderEvent.orderId, s"下单后15分钟之内支付的订单，order time: ${headCreateOrderEvent.timestamp}, paid time：${headPayOrderEvent.timestamp}")
  }
}