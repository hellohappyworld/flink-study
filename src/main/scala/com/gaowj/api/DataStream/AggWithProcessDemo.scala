package com.gaowj.api.DataStream

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by gaowj on 2019-06-03.
  * AggregateFunction/ProcessWindowFunction
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html#processwindowfunction-with-incremental-aggregation
  */
object AggWithProcessDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env) // 数据类型如：flink_media_f
    sourceDs.map(line => (line.split("\\t")(2), line.split("\\t")(14)))
      .filter(!_._2.contains("#"))
      .map(tup => (tup._1, tup._2.toLong))
      .keyBy(0)
      .timeWindow(Time.seconds(2))
      // AverageAggregate1计算的结果值传递给MyprocessWindowFunction
      .aggregate(new AverageAggregate1(), new MyprocessWindowFunction())
      //    ((113.69.35.152),(113.69.35.152,262,2,131.0))
      //    ((36.157.155.176),(36.157.155.176,262,2,131.0))
      //    ((114.223.160.182),(114.223.160.182,62,1,62.0))
      //    ((60.10.19.168),(60.10.19.168,67,1,67.0))
      //    ((180.117.31.224),(180.117.31.224,262,2,131.0))
      .print()


    env.execute("AggWithProcessDemo")
  }
}

// 计算tup(String,Long)的平均值
class AverageAggregate1 extends AggregateFunction[(String, Long), (String, Long, Long), (String, Long, Long, Double)] {
  // 初始化累加器
  override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

  // 添加累加器
  override def add(in: (String, Long), acc: (String, Long, Long)): (String, Long, Long) =
    (in._1, in._2 + acc._2, acc._3 + 1L)

  // 累加器合并
  override def merge(acc: (String, Long, Long), acc1: (String, Long, Long)): (String, Long, Long) =
    (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)

  // 返回结果值
  override def getResult(acc: (String, Long, Long)): (String, Long, Long, Double) =
    (acc._1, acc._2, acc._3, acc._2 / acc._3)
}

// 其中key为keyBy(0)所获取的key值
class MyprocessWindowFunction extends ProcessWindowFunction[(String, Long, Long, Double), (String, (String, Long, Long, Double)), Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, averages: Iterable[(String, Long, Long, Double)], out: Collector[(String, (String, Long, Long, Double))]): Unit = {
    val average: (String, Long, Long, Double) = averages.iterator.next()
    out.collect((key.toString, average))
  }
}
