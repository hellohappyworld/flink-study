package com.gaowj.api

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.common.functions.{FoldFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created on 2019-05-31.
  * RichMapFunction\SlidingProcessingTimeWindows\FoldFunction
  * original -> https://github.com/perkinls/flink-local-train/blob/7d70c5032809c33c13a111e5c4506c7c04b26ae1/src/main/scala/com/lp/test/windows/TumblingWindowsFold.scala
  */
object TumblingWindowsFoldDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000) // watermark时间间隔

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    sourceDs.map(new RichMapFunction[String, (String, Long)] {
      // 将一条数据切分，获得两个字段
      override def map(in: String): (String, Long) = {
        val arr: Array[String] = in.split("\\t")
        //        (arr(2), if (arr(11).equals("#")) 1 else arr(11).toLong)
        (arr(2), 1L)
      }
    })
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10))) // (窗口大小，滑动窗口大小)
      .fold("-->", new FoldFunction[(String, Long), String] {
      override def fold(acc: String, value: (String, Long)): String = {
        acc + value._2
      }
    })
      //    -->1
      //    -->11
      //    -->1111111
      //    -->1
      //    -->11111
      //    -->1
      .print()

    env.execute("TumblingWindowsFoldDemo")
  }

}
