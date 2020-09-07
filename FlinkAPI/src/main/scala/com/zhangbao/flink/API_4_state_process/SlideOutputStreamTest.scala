package com.zhangbao.flink.API_4_state_process

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 14:46 2020/9/2
 * @Description:  侧输出流测试
 *
 */
object SlideOutputStreamTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.socketTextStream("hadoop103", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    val resultStream: DataStream[SensorReading] = dataStream.process(new SpiltTempFunction(30.0))

    val slideOutputStream: DataStream[String] = resultStream.getSideOutput(new OutputTag[String]("high"))

    resultStream.print("low").setParallelism(1)

    slideOutputStream.print("high").setParallelism(1)
    environment.execute()
  }
}

class SpiltTempFunction(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
  private val highTag: OutputTag[String] = new OutputTag[String]("high")
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature >= threshold) {
      ctx.output(highTag,s"警告：${value.id}的温度为${value.temperature},过高")
    }else{
      out.collect(value)
    }
  }
}
