package com.zhangbao.flink.day03


import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Zhangbao
 * @Date: 8:17 2020/9/2
 * @Description:
 *
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.**定义环境的时间语义--例如，时间语义：EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkApi\\input.txt"
    val inputStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //2.设置EventTime时间戳，和Watermark
    val dataWithTimeDStream: DataStream[SensorReading] = dataStream.assignTimestampsAndWatermarks(
/*      new AssignerWithPeriodicWatermarks[SensorReading] {
      override def getCurrentWatermark: Watermark = ???
      override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = ???
    }*/
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp
      }
    )

    dataStream.assignAscendingTimestamps(_.timestamp * 1000)

    dataStream.assignTimestampsAndWatermarks(new MyAssigner(300))
    //3.对流进行分组
    val keyByStream: KeyedStream[SensorReading, String] = dataWithTimeDStream.keyBy(_.id)




    dataWithTimeDStream.print().setParallelism(1)

    //执行流的环境
    environment.execute("dataWithTime")

  }
}

class MyAssigner(mills:Long) extends AssignerWithPeriodicWatermarks[SensorReading]{

  override def getCurrentWatermark: Watermark = new Watermark(mills)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp.max(previousElementTimestamp)
}
