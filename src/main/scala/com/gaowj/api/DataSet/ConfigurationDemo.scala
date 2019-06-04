package com.gaowj.api.DataSet

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

/**
  * Created on 2019-06-04.
  * Configuration
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#via-withparametersconfiguration
  */
object ConfigurationDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val toFilter = env.fromElements(1, 2, 3, 4)

    val c: Configuration = new Configuration()
    c.setInteger("limit", 2) // 设置limit为2

    val ds: DataSet[Int] = toFilter.filter(new RichFilterFunction[Int] {
      var limit = 0

      override def open(config: Configuration): Unit = {
        limit = config.getInteger("limit", 0) // 获取limit值，获取不到置为0
      }

      override def filter(in: Int): Boolean = {
        in > limit
      }
    }).withParameters(c)

    //    3
    //    4
    ds.print()

  }
}
