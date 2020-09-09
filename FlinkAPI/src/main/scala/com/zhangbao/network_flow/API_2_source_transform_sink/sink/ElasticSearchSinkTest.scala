package com.zhangbao.network_flow.API_2_source_transform_sink.sink

import java.util

import com.zhangbao.network_flow.API_2_source_transform_sink.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @Author: Zhangbao
 * @Date: 0:51 2020/9/1
 * @Description:
 *  ES连接的使用：
 *  自定义实现
 */
object ElasticSearchSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val fileStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = fileStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    //配置主机和端口号
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop103", 9200))
    //添加sink输出数据到ES
    dataStream.addSink(
      new ElasticsearchSink.Builder[SensorReading](httpHosts, new MyElasticsearchSinkFunction())
    .build()
    )
    environment.execute("ES")
  }
}
//自定义MyElasticsearchSinkFunction，定义写入操作
class MyElasticsearchSinkFunction() extends ElasticsearchSinkFunction[SensorReading] {
  override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    //提取数据包装Source
    val hashMap: util.HashMap[String, String] = new util.HashMap()
    hashMap.put("id", element.id)
    hashMap.put("ts",element.timestamp.toString)
    hashMap.put("temp",element.temperature.toString)

    //创建Index请求，写入数据
    val request: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("_doc")
      .source(hashMap)

    //利用RequestIndexer发送http请求
    indexer.add(request)
  }
}


