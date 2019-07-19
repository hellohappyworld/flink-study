package com.gaowj.api.TableSQL

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

/**
  * Created on 2019-07-19.
  * TableEnvironment/connect
  * original -> https://github.com/duangduangda/Camel/blob/48fec2864af1aef1a9b4f4836bed72415a35b814/src/main/scala/org/dean/camel/table/FileSysConnector.scala
  */
object FileSysConnector {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val schema: Schema = new Schema()
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("age", Types.INT)

    tableEnv.connect(
      new FileSystem()
        .path("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv1")
    )
      .withFormat(
        new Csv()
          .fieldDelimiter(",")
          .lineDelimiter("\n")
          .field("id", Types.INT)
          .field("name", Types.STRING)
          .field("age", Types.INT)
          //          .ignoreFirstLine()
          .ignoreParseErrors()
      )
      .withSchema(schema)
      .registerTableSource("t_friend")

    val table: Table = tableEnv.scan("t_friend")
      //      .select("id,name,age")
      .select("*")

    tableEnv.toDataSet[Row](table).print()

  }
}
