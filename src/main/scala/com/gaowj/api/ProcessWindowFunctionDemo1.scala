package com.gaowj.api

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created on 2019-05-31.
  * ProcessWindowFunction
  * original --> https://github.com/tiantingting5435/flink/blob/4f13032cc92e42c79cc828ccef0911455609973a/src/main/scala/com/atguigu/keyedState/KeyedStateTest.scala
  */
object ProcessWindowFunctionDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaConsumerDemo.getKafkaData(env)
    val mapData = source.map(x => (x.split("\\t")(2), 1))

    val res = mapData
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .process(new MyProcessWindowFunctions())

    res.print()
    env.execute()
  }
}

class MyProcessWindowFunctions extends ProcessWindowFunction[(String, Int), Object, Tuple, TimeWindow] {
  //  var count = 0 // 有全量累加效果
  override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[Object]): Unit = {
    var count = 0 // 只统计窗口内的数据，不具有全量累加效果
    for (item <- elements) {
      count += 1
    }
    out.collect("key: " + key + "  -----> count:" + count)
  }
}