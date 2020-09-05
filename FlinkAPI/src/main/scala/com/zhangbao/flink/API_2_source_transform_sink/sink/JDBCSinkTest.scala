package com.zhangbao.flink.API_2_source_transform_sink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 0:53 2020/9/1
 * @Description: 测试自定义Sink
 * 自定义JDBC连接器：需要实现SinkFunction函数，这里继承了RichSinkFunction
 */
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val fileStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = fileStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    //添加自定义sink
    dataStream.addSink(new MyJDBCSink)

    environment.execute("JDBC")
  }
}

//自定义Sink连接JDBC，实现RichSinkFunction
class MyJDBCSink() extends RichSinkFunction[SensorReading] {
  //1.定义sql连接，预编译语句
  var connection: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  //2.注册连接实现预编译语句
  override def open(parameters: Configuration): Unit = {
    connection = DriverManager
      .getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456")
    insertStatement = connection.prepareStatement(
      """
        |INSERT INTO temperatures (sensor, temp)
        |VALUES (?, ?)
        |""".stripMargin)
    updateStatement = connection.prepareStatement(
      """
        |UPDATE temperatures
        |SET temp = ?
        |WHERE sensor = ?
        |""".stripMargin)
  }

  //3.直接执行更新语句，如果没有更新就插入
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStatement.setDouble(1, value.temperature)
    updateStatement.setString(2, value.id)
    updateStatement.execute()

    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.temperature)
      insertStatement.execute()
    }
  }

  //4.释放资源，关闭连接
  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    connection.close()
  }
}
