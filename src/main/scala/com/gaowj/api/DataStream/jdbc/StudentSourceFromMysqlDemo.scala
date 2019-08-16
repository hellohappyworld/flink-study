package com.gaowj.api.DataStream.jdbc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created on 2019-08-16.
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/src/main/scala/code/book/stream/customsinkandsource/jdbc/scala/StudentSourceFromMysqlTest.scala
  */
object StudentSourceFromMysqlDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[Student] = env.addSource(new StudentSourceFromMysql)

    source.print()

    env.execute("StudentSourceFromMysqlDemo")
  }
}
