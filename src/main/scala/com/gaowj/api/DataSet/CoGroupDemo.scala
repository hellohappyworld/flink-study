package com.gaowj.api.DataSet

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Created on 2019-06-04.
  * CoGroup
  * original -> https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/dataset_transformations.html#cogroup
  */
object CoGroupDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupArr = Array((1, "1-1"), (1, "1-2"), (1, "1-3"), (8, "1-1"), (8, "1-2"), (7, "1-1"), (7, "1-2"), (7, "1-3"))
    val input: DataSet[(Int, String)] = env.fromCollection(tupArr)
    val tupArr2 = Array((1, "2-1"), (1, "2-3"), (8, "2-1"), (8, "2-2"), (7, "2-2"), (7, "2-3"))
    val input2: DataSet[(Int, String)] = env.fromCollection(tupArr2)

    //    input.join(input2)
    //      .where(0).equalTo(0) {
    //      (input, input2, out: Collector[(Int, String, Int, String)]) =>
    //        // 此时的input,input2分别是符合join的一条数据
    //        out.collect((input._1, input._2, input2._1, input2._2))
    //    }
    //      //    (7,1-1,7,2-2)
    //      //    (7,1-2,7,2-2)
    //      //    (7,1-3,7,2-2)
    //      //    (7,1-1,7,2-3)
    //      //    (7,1-2,7,2-3)
    //      //    (7,1-3,7,2-3)
    //      //    (1,1-1,1,2-1)
    //      //    (1,1-2,1,2-1)
    //      //    (1,1-3,1,2-1)
    //      //    (1,1-1,1,2-3)
    //      //    (1,1-2,1,2-3)
    //      //    (1,1-3,1,2-3)
    //      //    (8,1-1,8,2-1)
    //      //    (8,1-2,8,2-1)
    //      //    (8,1-1,8,2-2)
    //      //    (8,1-2,8,2-2)
    //      .print()


    input.coGroup(input2)
      .where(0).equalTo(0) {
      (input, input2, out: Collector[(Int, String)]) =>
        // 此处input,input2是迭代器
        for (a <- input) {
          println(a._1, a._2)
        }
        for (b <- input2) {
          println(b._1, b._2)
        }
    }
      //    (7,1-1)
      //    (7,1-2)
      //    (7,1-3)
      //    (7,2-2)
      //    (7,2-3)
      //    (1,1-1)
      //    (1,1-2)
      //    (1,1-3)
      //    (1,2-1)
      //    (1,2-3)
      //    (8,1-1)
      //    (8,1-2)
      //    (8,2-1)
      //    (8,2-2)
      .collect()

  }
}
