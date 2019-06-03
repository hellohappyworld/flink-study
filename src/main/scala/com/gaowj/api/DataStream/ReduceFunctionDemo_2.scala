package com.gaowj.api.DataStream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._

/**
  * Created on 2019-05-30.
  * ReduceFunction..
  * original -> https://github.com/yunluwen/data_test/blob/9aa7dbc5797508e0487a6bc4bdf2a6e7d697b99d/flink_study/src/main/scala/com/zyh/batch/ReduceFunction001scala.scala#L40
  */
object ReduceFunctionDemo_2 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    text.reduce(new ReduceFunction[Int] {
      // 计算累加和
      override def reduce(t: Int, t1: Int): Int = {
        t + t1
      }
    }).print() // 28

    text.reduce(new ReduceFunction[Int] {
      override def reduce(t: Int, t1: Int): Int = {
        println(s"t=$t,t1=$t1")
        t + t1
      }
    }).collect()
    //    t=1,t1=2
    //    t=3,t1=3
    //    t=6,t1=4
    //    t=10,t1=5
    //    t=15,t1=6
    //    t=21,t1=7

  }
}
