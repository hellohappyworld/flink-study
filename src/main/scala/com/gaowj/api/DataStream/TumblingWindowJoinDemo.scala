package com.gaowj.api.DataStream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created on 2019-06-03.
  * TumblingEventTimeWindows/join
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/joining.html#tumbling-window-join
  */
object TumblingWindowJoinDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



    //    val orangeDs = KafkaConsumerDemo.getKafkaData(env)
    //      .map(line => (line.split("\\t")(2)))
    //    val greenDs = KafkaConsumerDemo1.getKafkaData(env)
    //      .map(line => (line.split("\\t")(2)))

    //    orangeDs.print()
    //    greenDs.print()

    //    orangeDs.join(greenDs)
    //      .where(_._1)
    //      .equalTo(_._1)
    //      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
    //      .apply { (a, b) => a }
    //      .print().setParallelism(1)

    //    val orangeDs = sourceDs.map(line => "#")
    //    val greenDs = sourceDs.map(line => "#")

    //    orangeDs.join(greenDs)
    //      .where(a => a)
    //      .equalTo(b => b)
    //      .window(TumblingEventTimeWindows.of(Time.milliseconds(3)))
    //      .apply(new JoinFunction[String, String, String] {
    //        override def join(in1: String, in2: String): String = in1 + "," + in2
    //      })
    //      .print()

    env.execute("TumblingWindowJoinDemo")
  }
}
