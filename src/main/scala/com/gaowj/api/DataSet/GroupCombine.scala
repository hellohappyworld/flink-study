package com.gaowj.api.DataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * Created on 2019-08-08.
  * Tuple1/combineGroup
  * original -> https://www.iteblog.com/archives/2069.html
  */
object GroupCombine {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[Tuple1[String]] = env.fromElements("a", "b", "c", "a").map(Tuple1(_))
    //    input.print()
    //    (a)
    //    (b)
    //    (c)
    //    (a)
    val combineWords: DataSet[(String, Int)] = input
      .groupBy(0)
      .combineGroup {
        (words, out: Collector[(String, Int)]) =>
          var key: String = null
          var count: Int = 0
          for (word <- words) {
            key = word._1
            count += 1
          }
          out.collect((key, count))
      }
    combineWords.print()
  }
}
