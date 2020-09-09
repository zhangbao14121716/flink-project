package com.zhangbao.network_flow.API_2_source_transform_sink.source

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 20:19 2020/9/6
 * @Description:
 * 从文件读取数据流
 */
object FileSourceTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    // 从文件读取数据流
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val inputStream: DataStream[String] = environment.readTextFile(inputPath)

    //结构转换
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim().toDouble)
    })

    dataStream.print("file")
    environment.execute("FileSource")
  }
}
