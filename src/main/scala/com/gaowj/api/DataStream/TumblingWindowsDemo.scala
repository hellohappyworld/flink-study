package com.gaowj.api.DataStream

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by gaowj on 2019-05-30.
  * TumblingProcessingTimeWindows/AggregateFunction
  * original -> https://github.com/itinycheng/flink-learn/blob/e838ed6902da0b0d893e44e832859f9e63af563b/api/src/main/scala/com/tiny/flink/streaming/window/TumblingWindow10.scala
  */
object TumblingWindowsDemo {

  // AggregateFunction 参数分别代表：输入数据类型，累加器数据类型，结束数据类型
  def aggregateFunction(): AggregateFunction[(Double, Long), (Double, Double, Long), (Double, Double, Long)] = {
    new AggregateFunction[(Double, Long), (Double, Double, Long), (Double, Double, Long)] {
      // 初始化累加器
      override def createAccumulator(): (Double, Double, Long) = (0, 0, 0)

      // 输入类型in,累加器类型acc
      override def add(in: (Double, Long), acc: (Double, Double, Long)): (Double, Double, Long) =
        (in._1 + acc._1, in._1 + acc._2, in._2 + acc._3)

      // 合并两个累加器
      override def merge(acc: (Double, Double, Long), acc1: (Double, Double, Long)): (Double, Double, Long) =
        (acc._1, acc._2, acc._3 + acc1._3)

      // 从累加器中提取输出结果
      override def getResult(acc: (Double, Double, Long)): (Double, Double, Long) = (acc._1, acc._2, acc._3)
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setParallelism(1)

    //消费出的KAFKA数据格式如：flink_media_f
    val ds: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    ds.flatMap(line => Array(line.split("\\t")(14), line.split("\\t")(11))).filter(!_.equals("#"))
      .map(str => (str.toDouble, 1L))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(aggregateFunction()).print()

    env.execute("TumblingWindowsDemo")
  }
}
