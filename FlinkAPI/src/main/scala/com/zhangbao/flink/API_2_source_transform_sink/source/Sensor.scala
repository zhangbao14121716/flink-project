package com.zhangbao.flink.API_2_source_transform_sink.source

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

/**
 * @Author: Zhangbao
 * @Date: 9:16 2020/8/31
 * @Description:
 *
 */
object Sensor {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    //1.从集合读取数据
//    val inputList: List[SensorReading] = List(
//      SensorReading("sensor_1", 1547718199, 35.8),
//      SensorReading("sensor_6", 1547718201, 15.4),
//      SensorReading("sensor_7", 1547718202, 6.7),
//      SensorReading("sensor_10", 1547718205, 38.1)
//    )
//    val inputStream1: DataStream[SensorReading] = environment.fromCollection(inputList)
//    inputStream1.print().setParallelism(1)

    //2.从文件读取数据
//    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkApi\\input.txt"
//    val inputStream2: DataStream[String] = environment.readTextFile(inputPath)

    //3.从kafka读取数据

//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "hadoop103:9092,hadoop102:9092,hadoop104:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//
//    val inputKafkaStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("Sensor", new SimpleStringSchema(), properties))
////    inputKafkaStream.print().setParallelism(1)
////    val inputStream3: DataStream[SensorReading] = environment.addSource(new MySourceFunction)
////    val resultDStream: SplitStream[SensorReading] = inputStream3.split(sr => {
////      if (sr.temperature > 30) List("high") else List("low")
////    })
//    val outputKafkaStream: DataStream[String] = inputKafkaStream.map(data => {
//      val arr: Array[String] = data.split(",")
//      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble).toString
//    })

    //4.sink输出到kafka
//    outputKafkaStream.addSink(
//      new FlinkKafkaProducer011[String]("hadoop103:9092", "sinktest", new SimpleStringSchema()
//      )
//    )
//    resultDStream.print().setParallelism(1)
    //5.自定义数据源
    environment.addSource(new MySourceFunction).print("mysource").setParallelism(1)
    environment.execute("kafka")
  }
}



//自定义数据源
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