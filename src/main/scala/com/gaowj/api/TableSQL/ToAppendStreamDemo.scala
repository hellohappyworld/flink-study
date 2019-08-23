package com.gaowj.api.TableSQL

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._


/**
  * Created on 2019-08-23
  * original -> https://github.com/liubin2048/flinkLearning/blob/1df22253a6138214f464bb5bbf39847e1ecb69fc/src/main/scala/com.liubin.flink/tableAndSQL/StreamSQLDemo.scala
  */
object ToAppendStreamDemo {

  def main(args: Array[String]): Unit = {


    //--------------------------------------------------------------------------------------------

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // convert DataStream to Table
    val tableA = tEnv.fromDataStream(orderA)
    tableA.toAppendStream[Order].print()

    // register DataStream as Table
    //    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)

    // union the two tables
    //    val result = tEnv.sqlQuery(
    //      s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
    //        "SELECT * FROM OrderB WHERE amount < 2")

    //    result.printSchema()

    //    val sink: CsvTableSink = new CsvTableSink("D:\\workStation\\Test\\flink-study\\1", fieldDelim = ",")
    //    result.writeToSink(sink)

    //    result.toAppendStream[Order].print()

    //    val tmpTable = result.toAppendStream[Order]
    //    tEnv.registerDataStream("tmpTable", tmpTable, 'user, 'product, 'amount)
    //    val result2 = tEnv.sqlQuery("SELECT * FROM tmpTable WHERE amount > 2 ")
    //    result2.toAppendStream[Order].print()
    //    result2.toAppendStream[Order].writeAsCsv("file:///home/hadoop/a.txt").setParallelism(1)
    //    tEnv.registerDataStream("TempTable",result)
    //    tEnv.sqlQuery(s"SELECT * FROM TempTable WHERE amount > 2 ").toAppendStream[Order].print()
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

}

