package com.gaowj.util

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaConsumerDemo {
  //  private val ZOOKEEPER_HOST: String = "10.80.11.157:2181 10.80.12.157:2181 10.80.13.157:2181"
  private val ZOOKEEPER_HOST: String = "10.80.28.154:2181,10.80.29.154:2181,10.80.30.154:2181"
  //  private val KAFKA_BROKER: String = "10.80.11.157:9092,10.80.12.157:9092,10.80.13.157:9092,10.80.14.157:9092,10.80.15.157:9092,10.80.16.157:9092,10.80.18.158:9092,10.80.20.158:9092,10.80.21.158:9092,10.80.22.158:9092,10.80.23.158:9092,10.80.24.158:9092,10.80.26.157:9092,10.80.27.157:9092"
  private val KAFKA_BROKER: String = "10.80.28.154:9092,10.80.29.154:9092,10.80.30.154:9092,10.80.31.154:9092,10.80.32.154:9092"
  private val TRANSACTION_GROUP: String = "swhdhweoghwgwudlsh"

  private val TOPIC: String = "app_explore_test"
  //  private val TOPIC: String = "app_news"


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
    prop.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "600000")
    prop.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](TOPIC, new SimpleStringSchema(), prop)
    kafkaConsumer.setStartFromLatest()
    val kafkaDStream: DataStream[String] = env.addSource(kafkaConsumer)

    kafkaDStream
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    getKafkaData(env)
      .print()

    env.execute("kafka consumer test")
  }
}
