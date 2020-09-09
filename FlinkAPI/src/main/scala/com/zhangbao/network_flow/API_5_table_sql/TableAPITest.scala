package com.zhangbao.network_flow.API_5_table_sql

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 9:57 2020/9/4
 * @Description:
 *  简单测试TableAPI
 *
 */
object TableAPITest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = environment.readTextFile("D:\\mybigdata\\workspace\\bigdataFlink\\FlinkApi\\input.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
      )

    //对Environment进行额外设置
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //从一条流创建一个表
    val dataTable: Table = tableEnvironment.fromDataStream(dataStream)

    //从表里选定特定的数据
    val selectedTable: Table = dataTable.select("id, timestamp")
      .filter("id = '\"sensor_1\"'")

    //对表进行流转换
    val resultStream: DataStream[(String, Long)] = selectedTable.toAppendStream[(String, Long)]

    resultStream.print()

    environment.execute("table api")
  }
}
