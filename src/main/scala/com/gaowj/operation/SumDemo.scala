package com.gaowj.operation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object SumDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val sourceDs = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190801\\newsapp_count")
    //    val sourceDs = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190801\\newsapp_info_count")
    val tupDs = sourceDs.map(line => {
      val arr = line.split("_")
      (arr(0).toLong, arr(1).toLong)
    })

    val filDs = tupDs.filter(tup => tup._1 >= 201908010000L && tup._1 < 201908020000L).map(_._2)
    //    println(filDs.count())
    val sumDs = filDs.reduce(_ + _)
    sumDs.print()
  }
}
