package com.gaowj.api.DataSet

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created jon 2019-06-03.
  * Bulk Iterations
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#bulk-iterations-1
  */
object IterrationDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val initial = env.fromElements(0)

    // iterate(10000) 迭代1000次
    val count: DataSet[Int] = initial.iterate(10000) { iterationInput: DataSet[Int] =>
      val result: DataSet[Int] = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    } // count仅包含一个数值

    val result: DataSet[Double] = count.map(c => c / 10000.0 * 4)
    result.print()
  }
}
