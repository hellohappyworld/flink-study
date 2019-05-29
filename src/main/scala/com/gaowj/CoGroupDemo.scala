package com.gaowj

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * Created by gaowj on 2019-05-29.
  * flink coGroup
  * reference https://github.com/mathiaspet/gms/blob/9cab16ced9ee68a8cb2713391f19db45858b16eb/flink-tests/src/test/scala/org/apache/flink/api/scala/operators/CoGroupITCase.scala
  */
object CoGroupDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //      1,11,1aaa
    //      2,22,2aaa
    //      3,33,3aaa
    //      3,33,4aaa
    val csvDs1: DataSet[(String, String, String)] = env.readCsvFile[Tuple3[String, String, String]](
      "D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv1",
      fieldDelimiter = ",",
      includedFields = Array(0, 1, 2)
    )
    //      1,11,1bbb
    //      2,22,2bbb
    //      3,33,3bbb
    val csvDs2: DataSet[(String, String, String)] = env.readCsvFile[Tuple3[String, String, String]](
      "D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv2",
      fieldDelimiter = ",",
      includedFields = Array(0, 1, 2)
    )

    val cogroupds = csvDs1.coGroup(csvDs2).where(0).equalTo(0) {
      (first, second) =>
        // 此处first second为迭代器
        var left: String = ""
        var right: String = ""
        for (t <- first) {
          val tt = t._1 + "_" + t._2 + "_" + t._3
          left = left + "||" + tt
        }
        for (t <- second) {
          val tt = t._1 + "_" + t._2 + "_" + t._3
          right = right + "||" + tt
        }
        (left, right)
    }

    //    (||3_33_3aaa||3_33_4aaa,||3_33_3bbb)
    //    (||1_11_1aaa,||1_11_1bbb)
    //    (||2_22_2aaa,||2_22_2bbb)
    cogroupds.print()

    /**
      * 迭代器first中含有满足条件的所有csvDs1的数据
      * 迭代器second中含有满足条件的所有csvDs2的数据
      */
  }
}
