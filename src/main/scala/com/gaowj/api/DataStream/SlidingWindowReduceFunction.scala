package com.gaowj.api.DataStream

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by gaowj on 2019-05-30
  * ReduceFunction\RichMapFunction
  * original -> https://github.com/perkinls/flink-local-train/blob/508c3d407d89f5b4b797b69998908783ba9e1109/src/main/scala/com/lp/test/windows/SlidingWindowsReduceFunction.scala
  */
object SlidingWindowReduceFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000) // watermark间隔时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    sourceDs.map(new RichMapFunction[String, (String, String)] {
      override def map(line: String): (String, String) = {
        val arr = line.split("\\t")

        (arr(0), arr(2))
      }
    }).windowAll(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(2)))
      .reduce(new ReduceFunction[(String, String)] {
        override def reduce(t1: (String, String), t2: (String, String)): (String, String) = {
          (t1._1 + "-->" + t2._1, t1._2 + "-->" + t2._2)
        }
      }).print()

    env.execute("SlidingWindowReduceFunction")
  }
}
