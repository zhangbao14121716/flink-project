package com.zhangbao

import java.lang
import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Author: Zhangbao
 * @Date: 17:02 2020/9/10
 * @Description:
 * 需求：布隆过滤器实现大数据量去重--日活分析
 * 处理逻辑：
 *
 *
 */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(8)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = environment.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId)) // map成二元组，只需要userId，还有dummy key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 自定义窗口触发规则
      .process(new UvCountResultWithBloom())


    environment.execute("bloomFilter distinct job ")
  }
}
// 实现自定义触发器，每个数据来了之后都触发一次窗口计算
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  //元素触发
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
  //ProcessTime触发
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //EventTime触发
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //清空
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

//实现自定义布隆过滤器,参数指定位图大小，并存放在Redis中，位图大小一般为2的次幂
class MyBloomFilter(size: Long) extends Serializable{
  //获取布隆过滤器位图大小
  val cap: Long = size
  //自定义hash函数，根据传入的种子和userId字串，生成随机hash值
  def hash(value: String, seed: Int): Long = {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    //cap-1 : 0000 0000 1111 1111
    //result: 1011 1101 1001 0011
    //结果 :  0000 0000 1001 0011 实在cap范围内的结果值
    (cap - 1) & result //返回范围的hash值

  }

}
//自定义函数，实现对于每个userId的去重判断
class UvCountResultWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  //创建Redis连接
  lazy private val jedis: Jedis = new Jedis("localhost", 6379)
  //创建布隆过滤器对象那个
  lazy private val bloomFilter: MyBloomFilter = new MyBloomFilter(1 << 29)
  //实现process方法
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //以windowEnd作为位图在Redis中的key
    val windowEnd: String = context.window.getEnd.toString
    //每来一个数据，就将它的userId进行hash运算，到redis的位图中判断是否存在
    val userIdString: String = elements.head._2.toString
    val offset: Long = bloomFilter.hash(userIdString, 61)
    val isExist: lang.Boolean = jedis.getbit(windowEnd, offset)
    //统计userId个数
    var count: Long = 0L
    //取出Redis中count对应的状态
    if( jedis.hget("count",windowEnd) != null )
      count = jedis.hget("count", windowEnd).toLong
    //如果存在，什么都不做；如果不存在，将对应位置1，count值加1
    if (!isExist) {
      jedis.setbit(windowEnd, offset, true)
      count += 1
      //把每个窗口的count保存到Reids的hash中
      jedis.hset("count",windowEnd,count.toString)

      //收集数据
      out.collect(UvCount(windowEnd.toLong, count))
    }
  }
}