package com.gaowj.api.DataStream.jdbc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by gaowj on 2019-08-15.
  * addSink to mysql
  */
object StudentSinkToMysqlDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(Student("gaowj", "nan", "China"))

    source.addSink(new StudentSinkToMysql)

    env.execute("StudentSinkToMysqlDemo")
  }
}
