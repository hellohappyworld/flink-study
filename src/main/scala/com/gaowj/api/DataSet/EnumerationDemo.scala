package com.gaowj.api.DataSet

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Created on 2019-06-03.
  * enumeration 可以读取嵌套文件夹文件，也可以读取同一子目录下的多个文件
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#data-sources
  */
object EnumerationDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val parameters: Configuration = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true) // 设置递归枚举参数

    val ds: DataSet[String] = env.readTextFile("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\media_f")
      .withParameters(parameters)
    //    这是文件2
    //    这是文件1
    //    这是文件3
    ds.print()

    val ds1: DataSet[String] = env.readTextFile("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\media_f\\dir")
      .withParameters(parameters)
    //    这是文件1
    //    这是文件2
    ds1.print()
  }
}
