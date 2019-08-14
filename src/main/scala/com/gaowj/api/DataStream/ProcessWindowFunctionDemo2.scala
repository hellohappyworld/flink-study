package com.gaowj.api.DataStream

import java.util.concurrent.TimeUnit

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

/**
  * Created on 2019-08-14.
  * keyBy/timeWindow/process
  * original -> http://shiyanjun.cn/archives/1775.html
  */
object ProcessWindowFunctionDemo2 {

  def checkParams(params: ParameterTool): Unit = {}

  def main(args: Array[String]): Unit = {
    // args : --window-size-millis 5000 --window-slide-millis 1000
    val params: ParameterTool = ParameterTool.fromArgs(args)
    checkParams(params)
    val windowSizeMillis: Long = params.getRequired("window-size-millis").toLong
    val windowSlideMillis: Long = params.getRequired("window-slide-millis").toLong

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val stream: DataStream[(String, String)] = KafkaConsumerDemo.getKafkaData(env).map(line => {
      val arr = line.split(",")
      (arr(0), arr(2))
    })

    //    val kafkaProducer = new FlinkKafkaProducer010[String](
    //      params.getRequired("window-result-topic"),
    //      new SimpleStringSchema(),
    //      params.getProperties
    //    )

    stream
      .map(t => {
        val ip = t._1
        val timeStr = t._2.split(" ")(0)
        ((ip, timeStr), 1L)
      })
      .keyBy(0)
      .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS), Time.of(windowSlideMillis, TimeUnit.MILLISECONDS))
      .process(new MyReduceWindowFunction)
      .map(t => {
        val key: (String, String, String, String) = t._1
        val count: Long = t._2
        val windowStartTime: String = key._1
        val windowEndTime: String = key._2
        val ip: String = key._3
        val timeStr: String = key._4
        Seq(windowStartTime, windowEndTime, ip, timeStr, count).mkString("\t")
      })
      //      .addSink(kafkaProducer)
      .print()

    env.execute("ProcessWindowFunctionDemo2")
  }
}










