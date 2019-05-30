package com.gaowj.api

import java.util.Properties

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * Created by gaowj on 2019-05-30
  * ReduceFunction，RichMapFunction
  * original -> https://github.com/perkinls/flink-local-train/blob/508c3d407d89f5b4b797b69998908783ba9e1109/src/main/scala/com/lp/test/windows/SlidingWindowsReduceFunction.scala
  */
object SlidingWindowReduceFunction {
  private val ZOOKEEPER_HOST: String = "*********:2181 *********:2181 *********:2181"
  private val KAFKA_BROKER: String = "*********:9092,*********:9092,*********:9092"
  private val TRANSACTION_GROUP: String = "ReduceFunction_test"

  private val TOPIC: String = "flink_media_f_online"


  /**
    * 消费KAFKA数据
    *
    * @param env
    */
  def getKafkaData(env: StreamExecutionEnvironment) = {
    val prop: Properties = new Properties()
    prop.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    prop.setProperty("bootstrap.servers", KAFKA_BROKER)
    prop.setProperty("group.id", TRANSACTION_GROUP)
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](TOPIC, new SimpleStringSchema(), prop)
    kafkaConsumer.setStartFromLatest()
    val kafkaDStream: DataStream[String] = env.addSource(kafkaConsumer)

    kafkaDStream
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000) // watermark间隔时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDs: DataStream[String] = getKafkaData(env)

    sourceDs.map(new RichMapFunction[String, (String, String)] {
      override def map(line: String): (String, String) = {
        val arr = line.split("\\t")

        (arr(0), arr(2))
      }
    }).windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
      .reduce(new ReduceFunction[(String, String)] {
        override def reduce(t1: (String, String), t2: (String, String)): (String, String) = {
          (t1._1 + "--" + t2._1, t1._2 + "--" + t2._2)
        }
      }).print()

    env.execute("SlidingWindowReduceFunction")
  }
}
