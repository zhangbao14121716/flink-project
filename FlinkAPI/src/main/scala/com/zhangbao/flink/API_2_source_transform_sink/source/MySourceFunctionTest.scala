package com.zhangbao.flink.API_2_source_transform_sink.source

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

/**
 * @Author: Zhangbao
 * @Date: 0:50 2020/9/5
 * @Description:
 * 自定义source测试
 * 实现SourceFunction
 */
object MySourceFunctionTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //添加自定义source
    val inputStream: DataStream[SensorReading] = environment.addSource(new MySourceFunction())

    inputStream.print().setParallelism(1)

    environment.execute()
  }
}

//自定义Source
class MySourceFunction extends SourceFunction[SensorReading]{
  var running: Boolean = true
  var count: Int = 0

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    //
    val random: Random = new Random()

    //
    var currentTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => {
      ("Sensor_" + i, 60 + random.nextGaussian() * 20)
    })
    //
    while (running) {
      if(count < 10) {
        currentTemp = currentTemp.map(temp => {
          (temp._1, temp._2 + random.nextGaussian())
        })

        val mills: Long = System.currentTimeMillis()
        currentTemp.foreach(temp => {
          sourceContext.collect(SensorReading(temp._1, mills, temp._2))
        })
        Thread.sleep(2000)
        count += 1
      } else cancel()
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
