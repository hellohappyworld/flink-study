package com.gaowj.api.DataSet

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet, _}
import org.apache.flink.util.Collector

/**
  * Created on 2019-08-15.
  * sortGroup/reduceGroup
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/book/api/dataset/dataset02.md
  */
object SortGroupDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      (20, "zhangsan"),
      (22, "zhangsan"),
      (22, "lisi"),
      (22, "lisi"),
      (22, "lisi"),
      (18, "zhangsan"),
      (18, "zhangsan")
    )

    val sortData: GroupedDataSet[(Int, String)] = input.groupBy(0).sortGroup(0, Order.ASCENDING)

    val outputDs: DataSet[(Int, String)] = sortData.reduceGroup(
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach (out.collect) // 将相同的元素使用Set去重
    )
    outputDs.print()
    //    (22,zhangsan)
    //    (22,lisi)
    //    (18,zhangsan)
    //    (20,zhangsan)
  }
}
