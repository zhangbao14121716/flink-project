package com.zhangbao.orderpay_detail

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 11:18 2020/9/9
 * @Description:
 * 需求：下单后15分钟的支付详情
 * 细粒度控制：自定义ProcessFunction（状态控制，计时器控制）
 * 1.输出15分钟内支付的
 * 2.输出15分钟以外支付的
 * 3.输出15分钟没有支付的
 * 4.输出没有创建订单，但有支付信息的
 */
object OrderPayTimeoutByProcess {
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

    //
    val resultStream: DataStream[OrderPayResult] = dataStream
      .keyBy(_.orderId)
      .process(new OrderPayDetectProcessFunction())

    resultStream.print("paid")
    resultStream.getSideOutput(new OutputTag[OrderPayResult]("paid-but-timeout")).print("paid-but-timeout")
    resultStream.getSideOutput(new OutputTag[OrderPayResult]("data-not-found")).print("data-not-found")
    resultStream.getSideOutput(new OutputTag[OrderPayResult]("time-out")).print("time-out")

    environment.execute("order pay timout with out cep job")
  }
}

class OrderPayDetectProcessFunction() extends KeyedProcessFunction[Long, OrderEvent, OrderPayResult] {
  //定义状态，用来保存是否create、pay事件
  lazy private val isPaidState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-paid", classOf[Boolean]))
  lazy private val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-create", classOf[Boolean]))
  //
  lazy private val timerTsStater: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
  //
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#Context, out: Collector[OrderPayResult]): Unit = {
    //
    val isCreated: Boolean = isCreateState.value()
    val isPaid: Boolean = isPaidState.value()
    val timerTs: Long = timerTsStater.value()

    //
    if(value.eventType == "create") {

      if(isPaid){
        //
        out.collect(OrderPayResult(value.orderId,"paid successfully"))
        //
        ctx.timerService().deleteEventTimeTimer(timerTs)
        //
        isPaidState.clear()
        isCreateState.clear()
        timerTsStater.clear()
      }else {
        //
        val ts: Long = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        //
        timerTsStater.update(ts)
        isCreateState.update(true)
      }
    }else if (value.eventType == "pay"){
      //
      if(isCreated){
        //
        if(value.timestamp * 1000L < timerTs) {
          //
          out.collect(OrderPayResult(value.orderId,"paid successfully"))
        }else {
          //
          ctx.output(new OutputTag[OrderPayResult]("paid-but-timeout"),OrderPayResult(value.orderId,"paid but timeout"))
        }
        ctx.timerService().deleteEventTimeTimer(timerTs)
        //
        isPaidState.clear()
        isCreateState.clear()
        timerTsStater.clear()
      }else {
        //
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        timerTsStater.update(value.timestamp * 1000L)
        isPaidState.update(true)
      }
    }
  }
  //
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#OnTimerContext, out: Collector[OrderPayResult]): Unit = {
    //
    if(isPaidState.value()){
      //
      ctx.output(new OutputTag[OrderPayResult]("data-not-found"), OrderPayResult(ctx.getCurrentKey,"paid but not cteate"))
    }else{
      //
      ctx.output(new OutputTag[OrderPayResult]("time-out"), OrderPayResult(ctx.getCurrentKey,"time out"))
    }
    //
    isPaidState.clear()
    isCreateState.clear()
    timerTsStater.clear()
  }
}
