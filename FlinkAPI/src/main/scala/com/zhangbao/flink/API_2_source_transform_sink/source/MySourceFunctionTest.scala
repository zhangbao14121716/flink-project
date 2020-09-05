package com.zhangbao.flink.API_2_source_transform_sink.source

import com.zhangbao.flink.API_2_source_transform_sink.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @Author: Zhangbao
 * @Date: 0:50 2020/9/5
 * @Description:
 *
 */
object MySourceFunctionTest {
  def main(args: Array[String]): Unit = {
    
  }
}
//自定义Source
class MySourceFunction extends SourceFunction[SensorReading] {

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

  }

  override def cancel(): Unit = {

  }
}
