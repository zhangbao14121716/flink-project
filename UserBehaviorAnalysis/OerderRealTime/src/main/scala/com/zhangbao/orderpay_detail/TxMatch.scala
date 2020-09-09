package com.zhangbao.orderpay_detail

import java.net.URL

import com.zhangbao.orderpay_detail.OrderPayTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 14:27 2020/9/9
 * @Description:
 *
 */
case class ReceiptEvent(txId: String, appName: String, timestamp: Long)

//
case class JoinOrderReceiptEvent()

object TxMatch {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //
    environment.setParallelism(1)
    //
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderUrlPath: URL = getClass.getResource("/OrderLog.csv")
    val receiptUrlPath: URL = getClass.getResource("/ReceiptLog.csv")
    val inputOrderStream: DataStream[String] = environment.readTextFile(orderUrlPath.getPath)
    val inputReceiptStream: DataStream[String] = environment.readTextFile(receiptUrlPath.getPath)

    //
    val dataOrderStream: DataStream[OrderEvent] = inputOrderStream.map(line => {
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).filter(_.eventType != "create")
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //
    val dataReceiptStream: DataStream[ReceiptEvent] = inputReceiptStream.map(line => {
      val arr: Array[String] = line.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = dataOrderStream.connect(dataReceiptStream)
      .keyBy(_.txId, _.txId)
      .process(new TxMatchProcessFunction())

    resultStream.print("--")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("single-order")).print("==")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("single-receipt")).print("..")

    //
    environment.execute("match ")
  }
}

//
class TxMatchProcessFunction() extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //
  lazy private val orderEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("last-order", classOf[OrderEvent]))
  //
  lazy private val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("last-receipt", classOf[ReceiptEvent]))

  //
  override def processElement1(orderEvent: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //
    val receiptEvent: ReceiptEvent = receiptEventState.value()
    if (receiptEvent != null) {
      //
      out.collect((orderEvent, receiptEvent))
      //
      orderEventState.clear()
      receiptEventState.clear()
    } else {
      //
      orderEventState.update(orderEvent)
      //
      ctx.timerService().registerEventTimeTimer(orderEvent.timestamp * 1000L + 5000L)
    }
  }

  //
  override def processElement2(receiptEvent: ReceiptEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //
    val orderEvent: OrderEvent = orderEventState.value()
    //
    if (orderEvent != null) {
      //
      out.collect((orderEvent, receiptEvent))
      //
      orderEventState.clear()
      receiptEventState.clear()
    } else {
      //
      receiptEventState.update(receiptEvent)
      //
      ctx.timerService().registerEventTimeTimer(receiptEvent.timestamp * 1000L + 5000L)
    }
  }

  //
  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //
    if (orderEventState.value() != null) {
      ctx.output(new OutputTag[OrderEvent]("single-order"), orderEventState.value())
    }
    if (receiptEventState.value() != null) {
      ctx.output(new OutputTag[ReceiptEvent]("single-receipt"), receiptEventState.value())
    }
    //
    orderEventState.clear()
    receiptEventState.clear()
  }
}
