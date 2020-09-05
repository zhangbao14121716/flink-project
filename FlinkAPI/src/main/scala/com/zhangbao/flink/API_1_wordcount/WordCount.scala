package com.zhangbao.flink.API_1_wordcount

import org.apache.flink.api.scala._

/**
 * @Author: Zhangbao
 * @Date: 11:37 2020/8/29
 * @Description: 批处理wordcount，使用（DataSetAPI）
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个批处理执行环境（DataSetAPI）
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.从文件读取数据
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\hello.txt"
    val inputDataSet: DataSet[String] = environment.readTextFile(inputPath)

    //3.批处理数据
    val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //4.输出
    resultDataSet.print()
  }
}
