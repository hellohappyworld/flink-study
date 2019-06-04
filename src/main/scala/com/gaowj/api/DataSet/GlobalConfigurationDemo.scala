package com.gaowj.api.DataSet

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Created on 2019-06-04.
  * Setting a custom global configuration.
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#globally-via-the-executionconfig
  */
object GlobalConfigurationDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val conf: Configuration = new Configuration()
    conf.setString("myKey", "myValue")
    env.getConfig.setGlobalJobParameters(conf)

    val ds = env.fromElements(1, 2, 3, 4)

    ds.map(new RichMapFunction[Int, (Int, String)] {
      var conff = "haha"

      override def open(globalConfig: Configuration): Unit = {
        conff = globalConfig.getString("myKey", null)
      }

      override def map(in: Int): (Int, String) = {
        (in, conff)
      }
    }).withParameters(conf)
      //    (1,myValue)
      //    (2,myValue)
      //    (3,myValue)
      //    (4,myValue)
      .print()

  }
}
