package com.zhangbao.network_flow.API_2_source_transform_sink.transform_richfunction

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Zhangbao
 * @Date: 23:47 2020/8/31
 * @Description:
 * 各种算子，对数据流进行各种转换
 * 1.算子种类
 *   map/flatMap/filter/(split,select)/(connect,comap)/unoin
 * 2.自定义实现个算子的utf函数
 * 3.自定义富函数（富函数由有生命周期的概念）
 *  任意函数算子都有相应的富函数
 *   3.1 open()
 *   3.2 close()
 *   3.3 getRuntimeContext():可以获得函数执行的并行度，任务的名字，以及state状态
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    // 读取数据
    val filePath: String = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = environment.readTextFile(filePath)

    // 1. 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr: Array[String] = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )
//      .filter( data => data.id.startsWith("sensor_1") )
      .filter( new MyFilter("sensor_1") )

    // 2. 分组聚合
    // 2.1 简单滚动聚合，求每一个传感器所有温度值的最小值
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .minBy("temperature")

    // 2.2 一般化聚合，输出(id, 最新时间戳 + 1，最小温度值)
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce( (curState, newData) =>
        SensorReading(newData.id, newData.timestamp + 1, curState.temperature.min(newData.temperature))
      )

    // 3. 多流转换
    // 3.1 分流(split-select)
    val splitStream: SplitStream[SensorReading] = dataStream
      .split( data => {
        if( data.temperature > 30 ) List("high") else List("low")
      } )

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    // 3.2 连接两条流(connect-(map,flatmap))
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature) )
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect( lowTempStream )
    val resultStream: DataStream[Any] = connectedStreams
        .map(//分别对两条流进行操作，最后合并
          warningData => (warningData._1, warningData._2, "high temp warning"),
          lowTempData => (lowTempData.id, "low temp")
        )

    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream, allTempStream)

    dataStream.print("data")
//    reduceStream.print("reduce")
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")

//    resultStream.print("result")

    environment.execute("transform test")
  }
}

// 自定义函数类，重写父类中的方法
class MyMapper extends MapFunction[SensorReading, (String, Double)]{
  override def map(value: SensorReading): (String, Double) = (value.id, value.temperature)
}
class MyFilter(keyword: String) extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = value.id.startsWith(keyword)
}


// 自定义flatMap富函数
class MyRichFlatMapper extends RichFlatMapFunction[SensorReading, String]{
  //声明周期初始化，会在flatMap方法之前调用
  override def open(parameters: Configuration): Unit = super.open(parameters)
  //实现flatMapFunction中的flatMap方法
  override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
    out.collect(value.id)
    out.collect("value")
  }
  //关闭生命周期，在最后调用
  override def close(): Unit = super.close()
}