package com.gaowj.api.TableSQL

import java.{io, lang}

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.InMemoryExternalCatalog
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row


/**
  * Created on 2019-08-20
  * original -> https://flink.sojb.cn/dev/table/common.html
  */
object TableDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //注册表
    // register a Table
    //    tableEnv.registerTable("table1", Table1)
    //    tableEnv.registerTableSource("table2",tableSource)
    //    tableEnv.registerExternalCatalog("txtCat",externalCatalog)
    // create a Table from a Table API query
    //    val tapiResult: Table = tableEnv.scan("table1").select("select * from table1")
    // create a Table from
    //    val sqlResult: Table = tableEnv.sqlQuery("select * from table2")
    // emit a Table API result Table to a TableSink, same for SQL result
    //    tapiResult.writeToSink(TableSink)


    //注册TableSource
    // create a TableSource
    //    val csvSource: TableSource = new CsvTableSource("/path/to/file", ...)
    // register the TableSource as table "CsvTable"
    //    tableEnv.registerTableSource("CsvTable",csvSource)


    //注册TableSink
    // create a TableSink
    //    val csvSink: TableSink = new CsvTableSink("/path/to/file",...)
    // define the fieId names and types
    //    val fieldNames: Array[String] = Array("a", "b", "c")
    //    val fieldTypes = Array(Types.INT, Types.STRING, Types.LONG)
    // register the TableSink as table "CsvSinkTable"
    //    tableEnv.registerTableSink("CsvSinkTable",fieldNames,fieldTypes,csvSink)


    // 查询表
    // scan registered Orders table
    //    val orders = tableEnv.scan("Orders")
    // compute revenue for all customers from France
    //    val revenue = orders
    //      .filter('cCountry === "FRANCE")
    //      .groupBy('cID, 'cName)
    //      .select('cID, 'cName, 'revenue.sum AS 'revSum)

    //    val revenue: Table = tableEnv.sqlQuery(
    //      """
    //select cID,cName,sum(revenue) as revSum
    //from Orders
    //where cCountry = 'FRANCE'
    //group by cID,cName
    //      """.stripMargin)


    //发射表
    //    val result: Table = tableEnv.sqlQuery("select * from result")
    //    val sink: CsvTableSink = new CsvTableSink("path", fieldDelim = "|")
    //    result.writeToSink(sink)
    //
    //    val fieldNames: Array[String] = Array("a", "b", "c")
    //    val fieldTypes = Array(Types.INT, Types.STRING, Types.LONG)
    //    tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink)
    //    result.insertInto("CsvSinkTable")


    //将DataStream或DataSet注册为表
    //    val stream: DataStream[(Int, String)] = env.fromElements((1, "test1"), (2, "test2"))
    //    tableEnv.registerDataStream("mytable1", stream)
    //    tableEnv.registerDataStream("myTable2", stream, 'myLong, 'myString)


    //将DataStream或DataSet转换为表
    //    val stream: DataStream[(Int, String)] = env.fromElements((1, "test1"), (2, "test2"))
    //    val table1: Table = tableEnv.fromDataStream(stream)
    //    val table2: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)


    //将表转换为DataStream
    val ds: DataStream[(Int, String, Int)] = env.fromElements((1, "liming", 23), (2, "wanghong", 34))
    ds.print()
    //    tableEnv.registerDataStream("addTable", ds, 'id, 'numbers, 'word)
    //    val table: Table = tableEnv.sqlQuery("select * from addTable")
    //    val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)
    //    dsRow.print()
    //    val dsTuple: DataStream[(String, Int)] = tableEnv.toAppendStream[(String, Int)](table) // Table仅通过INSERT更改修改时才能使用此模式
    //    val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)

    env.execute()
  }
}
