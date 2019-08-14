package com.gaowj.api.DataStream

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

    ProcessWindowFunction会暂存Window下所有的记录，然后进行计算。从这个角度来说ProcessWindowFunction能处理的情况更多。
  * 但是同时会占用更多的性能和资源。
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


/**
  * ProcessWindowFunction类型参数说明：IN, OUT, KEY, W extends Window
  * IN：输入数据流记录的类型
  * OUT：输出数据流记录的类型
  * KEY：The key parameter is the key that is extracted via the KeySelector that was specified for the keyBy() invocation.
  * In case of tuple-index keys or string-field references this key type is always Tuple and you have to manually cast it to a tuple of the correct size to extract the key fields.
  * Window：窗口的类型
  */
class MyProcessWindowFunction extends ProcessWindowFunction[(String, String), String, Tuple, TimeWindow] {

  override def process(key: Tuple, context: Context, input: Iterable[(String, String)], out: Collector[String]): Unit = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"key --> $key  count --> $count")
  }
}