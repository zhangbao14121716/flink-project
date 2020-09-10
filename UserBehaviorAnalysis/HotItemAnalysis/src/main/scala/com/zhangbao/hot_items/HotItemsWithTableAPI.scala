package com.zhangbao.hot_items

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
 * @Author: Zhangbao
 * @Date: 17:52 2020/9/9
 * @Description:
 *  需求：用Flink的TableAPI或SQL实现热门商品统计
 *  逻辑
 *  1、过滤、开窗、分组、查询
 *    fromDataStream
 *  2、SQL语句
 */
object HotItemsWithTableAPI {
  def main(args: Array[String]): Unit = {
    //创建流环境，配置环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取数据
    val urlPath: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = environment.readTextFile(urlPath.getPath)
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( line => {
        val arr: Array[String] = line.split(",")
        UserBehavior( arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong )
      } )

      .assignAscendingTimestamps( _.timestamp * 1000L )
    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)
    //从流中获取表
    val dataTable: Table = tableEnvironment.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    //用TableAPI对窗口进行聚合--过滤，开窗，分组，查询
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hour every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

    //将aggTable以追加的形式转化成流输出
//    aggTable.toAppendStream[Row].print("agg")
    //用SQL排序输出
    //创建临时视图
    tableEnvironment.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
    //SQL语句
    val resultTable: Table = tableEnvironment.sqlQuery(
      """
        |select *
        |from (
        | select *,
        |   row_number() over
        |   (partition by windowEnd order by cnt desc)
        |   as row_num
        | from agg
        |)
        |where row_num <= 5
        |""".stripMargin)
    //将resultTable以Retract的形式转化成流输出
    resultTable.toRetractStream[Row].print("res")

    environment.execute("sql job")
  }
}
