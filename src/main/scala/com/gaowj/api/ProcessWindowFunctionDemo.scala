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
  * ProcessWindowFunction.
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html#processwindowfunction
  */
object ProcessWindowFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDs = KafkaConsumerDemo.getKafkaData(env)
    sourceDs.map(line => {
      val arr = line.split("\\t")
      (arr(2), arr(19))
    }).keyBy(0)
      .timeWindow(Time.seconds(2))
      .process(new MyProcessWindowFunction())
      .print()


    env.execute("ProcessWindowFunctionDemo")
  }
}


// ProcessWindowFunction中参数分别为[(元素数据类型),输出结果类型,key数据类型,时间窗口]
class MyProcessWindowFunction extends ProcessWindowFunction[(String, String), String, Tuple, TimeWindow] {

  override def process(key: Tuple, context: Context, input: Iterable[(String, String)], out: Collector[String]): Unit = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"key --> $key  count --> $count")
  }
}