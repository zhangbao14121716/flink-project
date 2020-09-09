package com.zhangbao.network_flow.API_1_wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 11:51 2020/8/29
 * @Description: 流处理wordcount
 *  导包时需要隐式类型转换
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.使用sokcet接受通信
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")
    val inputDataStream: DataStream[String] = environment.socketTextStream(host, port)

    //设置全局并行度
    environment.setParallelism(1)

    //3.处理DataStream流
    val resultDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      //设置共享组，以后的默认都进该组
      //.slotSharingGroup("filter")
      //开启新的链
      //.startNewChain()
      //链无效化
      //.disableChaining()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //4.打印输出--可以设置并行度
    resultDataStream.print().setParallelism(1)

    //5.启动执行任务
    environment.execute("WordCount")
  }
}