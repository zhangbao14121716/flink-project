package com.zhangbao.network_flow.API_5_table_sql

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
 * @Author: Zhangbao
 * @Date: 9:57 2020/9/4
 * @Description:
 *  一般化的Table处理流程
 *
 */
object NormalTableAPI {
  def main(args: Array[String]): Unit = {
    //流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //表处理环境
    //老版本的表处理环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      //.inBatchMode()
      .inStreamingMode()
      .build()
//    //新版本的表处理环境
//    EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      //.inBatchMode()
//      .inStreamingMode()
//      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment,settings)
    val inputStream: DataStream[String] = environment.readTextFile("D:\\mybigdata\\workspace\\bigdataFlink\\FlinkApi\\input.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
    tableEnvironment.fromDataStream(dataStream)

    environment.execute("table api")
  }
}
