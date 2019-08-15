package com.gaowj.api.DataSet

import org.apache.flink.api.scala.{CrossDataSet, ExecutionEnvironment, _}

/**
  * Created on 2019-08-15.
  * cross
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/book/api/dataset/dataset03.md
  */
object CrossDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val one = env.fromElements((1, 4, 7), (2, 5, 8), (3, 6, 9))
    val two = env.fromElements((10, 40, 70), (20, 50, 80), (30, 60, 90))

    val out: CrossDataSet[(Int, Int, Int), (Int, Int, Int)] = one.cross(two)
    out.print()
    //    ((1,4,7),(10,40,70))
    //    ((1,4,7),(20,50,80))
    //    ((1,4,7),(30,60,90))
    //    ((2,5,8),(10,40,70))
    //    ((2,5,8),(20,50,80))
    //    ((2,5,8),(30,60,90))
    //    ((3,6,9),(10,40,70))
    //    ((3,6,9),(20,50,80))
    //    ((3,6,9),(30,60,90))
  }
}
