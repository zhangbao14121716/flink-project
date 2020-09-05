package com.zhangbao.flink.day04

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 9:29 2020/9/2
 * @Description:
 *
 */
object StateTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.socketTextStream("hadoop103", 7777)

    val dataDStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    val keyByDStream: KeyedStream[SensorReading, String] = dataDStream.keyBy(_.id)

    val resultDStream: DataStream[(String, Double, Double)] = keyByDStream.flatMap(new TempChangWarning(10.0))

    resultDStream.print().setParallelism(1)

    environment.execute("state test job")
  }
}

class TempChangWarning(warningTemp: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
//  //定义初始类型
//  private var lastTempState : ValueState[Double] = _
//
//  //初始生命周期
//  override def open(parameters: Configuration): Unit = {
//    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-Temp", classOf[Double]))
//  }

  //----懒执行
  lazy private val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))

  lazy private val isOccurState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("last-occur", classOf[Boolean]))
  //实现flatMap细粒度的操作，更新状态
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    if (isOccurState.value() && ((value.temperature - lastTempState.value()).abs >= warningTemp)) {
      out.collect((value.id, lastTempState.value(), value.temperature))
    }
    this.lastTempState.update(value.temperature)
    this.isOccurState.update(true)
  }
}