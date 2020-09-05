package com.zhangbao.flink.API_2_source_transform_sink.sink

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 18:12 2020/9/1
 * @Description: sink
 *  以文件形式输出
 */
object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val fileStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = fileStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    //1.以文件的形式输出到本地
    dataStream.writeAsCsv("D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\output\\output.txt")
    //2.以文件形式输出到本地或者远程
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\output\\output1.txt"),
        //new Path("hdfs://hadoop103:8020/user"),
        new SimpleStringEncoder[SensorReading]("UTF-8")
      ).build()
    )
    environment.execute("File/hdfs")
  }
}
