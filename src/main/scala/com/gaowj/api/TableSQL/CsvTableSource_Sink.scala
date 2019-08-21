package com.gaowj.api.TableSQL

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

/**
  * Created on 2019-08-21
  * CsvTableSource/CsvTableSink
  * original -> https://github.com/bkdubey/Flink_POC/blob/d13b5eb7d46c74d7b1a0ca402c9be553d1424ef9/src/main/scala/tableapi_1.scala
  */
object CsvTableSource_Sink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val csvSource: CsvTableSource = CsvTableSource
      .builder()
      .path("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv1")
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("age", Types.LONG)
      .fieldDelimiter(",")
      .build()

    tableEnv.registerTableSource("csvTable", csvSource)

    val orders: Table = tableEnv.scan("csvTable")
    //    orders.printSchema()
    //
    //    root
    //    |-- id: Integer
    //    |-- name: String
    //    |-- age: Long
    val res: Table = tableEnv.sqlQuery("select * from csvTable")
    //    res.printSchema()
    //
    //    root
    //    |-- id: Integer
    //    |-- name: String
    //    |-- age: Long

    res.writeToSink(
      new CsvTableSink(
        "D:\\workStation\\Test\\logParse\\appsta\\src20190820\\table.txt",
        fieldDelim = "|", // 分隔符
        //        numFiles = 1, // 结果写出到1个文件里
        numFiles = 2, // 结果写出到2个文件里
        writeMode = WriteMode.OVERWRITE
      )
    )

    env.execute()
  }
}
