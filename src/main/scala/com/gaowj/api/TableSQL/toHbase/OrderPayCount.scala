package com.gaowj.api.TableSQL.toHbase

import java.sql.Timestamp

import com.gaowj.api.TableSQL.toHbase.UDF.Add
import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Created on 2019-08-26
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/sql/OrderPayCount.scala
  */

// 有效订单统计金额及订单量统计
object OrderPayCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    val orders: Table = sourceDs.map(line => {
      val arr = line.split("\\t")
      Orders(arr(1).split("\\.")(2).toInt, arr(6), arr(1).split("\\.")(0).toInt, arr(1).split("\\.")(1).toDouble, 1.toInt, arr(1).split("\\.")(3).toInt, new Timestamp(arr(8).toLong))
    })
      .assignAscendingTimestamps(_.orderTime.getTime)
      .toTable(tEnv, 'orderId, 'productName, 'productNum, 'amount, 'isPay, 'categoryId, 'orderTime.rowtime)

    tEnv.registerFunction("add", new Add(3))

    tEnv.registerTable("Orders", orders)

    val sql: String =
      """
    select
     HOP_START(orderTime, INTERVAL '30' SECOND, INTERVAL '1' MINUTE),
     add(categoryId),
     count(orderId),
     sum(productNum*amount)
     from Orders
     where isPay=1
     group by HOP(orderTime, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), categoryId
        """.stripMargin

    val resultTable: Table = tEnv.sqlQuery(sql)

    resultTable.toAppendStream[OrderAmount]
      //      .addSink(new HBaseSink)
      .print()

    env.execute("OrderPayCount")
  }

  case class Orders(orderId: Int, productName: String, productNum: Int, amount: Double, isPay: Int, categoryId: Int, orderTime: Timestamp)

  case class OrderAmount(dt: Timestamp, categoryId: Int, orderNum: Long, orderAmount: Double)

}
