package com.zhangbao.network_flow.API_4_state_process

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.{lang, util}

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Author: Zhangbao
 * @Date: 11:35 2020/9/2
 * @Description:  侧输出流测试
 *
 */
object KeyedProcessTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.socketTextStream("hadoop103", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
  }
}
class MyKeyProcess extends KeyedProcessFunction[String, SensorReading, Int]{

  lazy private val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("list", classOf[Int]))
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    listState.add(10)

    listState.update(new util.ArrayList[Int])

    ctx.timerService().currentWatermark()

    ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 10)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, Int]#OnTimerContext, out: Collector[Int]): Unit = super.onTimer(timestamp, ctx, out)


}

class TempIncreaseWarning(interval : Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{

  lazy private val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy private val currentState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //取出状态
    val lastTemp: Double = lastTempState.value()
    val current: Long = currentState.value()

    //
    if (value.temperature > lastTemp && current == 0) {
      val ts: Long = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      currentState.update(ts)
    }else if(value.temperature <= lastTemp){
      ctx.timerService().deleteEventTimeTimer(current)
      currentState.clear()
    }
  }
    //事件触发器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(s"传感器${ctx.getCurrentKey}")

  }
}

