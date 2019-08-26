package com.gaowj.api.TableSQL.toHbase.UDF

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
  * Created on 2019-08-26
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/sql/udf/Add.scala
  */

// 自定义ADD函数
class Add(factor: Int = 2) extends ScalarFunction {
  def eval(s: Int): Int = s + 1000 * factor
}

case class AddData(id: Int, numbers: String, word: String)

object AddTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data1: Seq[AddData] = Seq(
      AddData(1001, "11030,1031", "abcd"),
      AddData(1002, "2030", "abc")
    )

    val addDs: DataStream[AddData] = env.fromCollection[AddData](data1)

    tEnv.registerDataStream("addTable", addDs, 'id, 'numbers, 'word)

    tEnv.registerFunction("add", new Add(3))
    tEnv.registerFunction("len", new StringLength())

    val sql: String =
      """
select
 add(id),
 len(numbers,word)
 from addTable
    """.stripMargin

    val result: Table = tEnv.sqlQuery(sql)

    val resultDs: DataStream[Row] = result.toAppendStream[Row]
    resultDs.print()

    env.execute("AddTest")
  }
}