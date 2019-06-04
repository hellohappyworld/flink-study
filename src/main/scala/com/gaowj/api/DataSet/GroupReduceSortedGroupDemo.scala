package com.gaowj.api.DataSet


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Created on 2019-06-04.
  * GroupReduce on sorted groups
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/dataset_transformations.html#groupreduce-on-sorted-groups
  * 该示例删除按整数1分组并按整数2排序的数据集中的重复数据。
  */
object GroupReduceSortedGroupDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupArr = Array((1, 1), (1, 1), (1, 3), (8, 6), (8, 5), (8, 78), (7, 1), (7, 4), (7, 0))
    val input: DataSet[(Int, Int)] = env.fromCollection(tupArr)

    // 注意：sortGroup是对组内元素排序
    input.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
      (in, out: Collector[(Int, Int)]) =>
        var prev: (Int, Int) = null
        for (t <- in) {
          if (prev == null || prev != t)
            out.collect(t)
          prev = t
        }
    }
      //    (7,0)
      //    (7,1)
      //    (7,4)
      //    (1,1)
      //    (1,3)
      //    (8,5)
      //    (8,6)
      //    (8,78)
      .print()

  }
}
