package com.zhangbao.flink.API_2_source_transform_sink.sink

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author: Zhangbao
 * @Date: 23:44 2020/8/31
 * @Description:
 * Redis的连接器使用
 * 需要自定义RedisMapper实现命令
 */
object RedisTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputPath: String = "D:\\mybigdata\\workspace\\bigdataFlink\\FlinkAPI\\input\\input.txt"
    val inputStream: DataStream[String] = environment.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    //连接Redis数据库配置信息
    val flinkJedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop103").setPort(6379).build()
    //添加Redis sink，时将数据输出写入到Redis
    dataStream.addSink(new RedisSink[SensorReading](flinkJedisPoolConfig, new MyRedisMapper))

    environment.execute("Redis")
  }
}

//自定义MyRedisMapper继承RedisMapper，执行写入操作
class MyRedisMapper() extends RedisMapper[SensorReading] {
  //1.Redis命令描述器，设置HSET命令，用来向HASH表中存放数据
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "Sensor_temperature")
  }
  //2.从数据中获取Hash表的Key
  override def getKeyFromData(t: SensorReading): String = t.id
  //2.从数据中获取Hash表的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
