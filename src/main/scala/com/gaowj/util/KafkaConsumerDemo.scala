package com.gaowj.util

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaConsumerDemo {
  private val ZOOKEEPER_HOST: String = "*********:2181 *********:2181 *********:2181"
  private val KAFKA_BROKER: String = "*********:9092,*********:9092,*********:9092"
  private val TRANSACTION_GROUP: String = "TumblingWindowsDemo_test"

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
}
