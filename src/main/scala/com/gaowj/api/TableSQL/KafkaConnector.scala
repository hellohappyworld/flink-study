package com.gaowj.api.TableSQL

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.descriptors.{Kafka, Rowtime, Schema}


/**
  * Created on 2019-08-21
  * original -> https://flink.sojb.cn/dev/table/connect.html#top
  */
object KafkaConnector {
  def main(args: Array[String]): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val tableEnvironment: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //
    //
    //    tableEnvironment
    //      // declare the external system to connect to
    //      .connect(
    //      new Kafka()
    //        .version("0.10")
    //        .topic("appsta")
    //        .startFromEarliest()
    //        .property("zookeeper.connect", "10.80.28.154:2181,10.80.29.154:2181,10.80.30.154:2181")
    //        .property("bootstrap.servers", "10.80.28.154:9092,10.80.29.154:9092,10.80.30.154:9092,10.80.31.154:9092,10.80.32.154:9092")
    //    )
    //
    //      // declare a format for this system
    //      .withFormat(
    //      new Avro()
    //        .avroSchema(
    //          "{" +
    //            "  \"namespace\": \"org.myorganization\"," +
    //            "  \"type\": \"record\"," +
    //            "  \"name\": \"UserMessage\"," +
    //            "    \"fields\": [" +
    //            "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
    //            "      {\"name\": \"user\", \"type\": \"long\"}," +
    //            "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
    //            "    ]" +
    //            "}"
    //        )
    //    )
    //
    //      // declare the schema of the table
    //      .withSchema(
    //      new Schema()
    //        .field("rowtime", Types.SQL_TIMESTAMP)
    //        .rowtime(new Rowtime()
    //          .timestampsFromField("ts")
    //          .watermarksPeriodicBounded(60000)
    //        )
    //        .field("user", Types.LONG)
    //        .field("message", Types.STRING)
    //    )
    //
    //      // specify the update-mode for streaming tables
    //      .inAppendMode()
    //
    //      // register as source, sink, or both and under a name
    //      .registerTableSource("MyUserTable")
    //
    //    val table: Table = tableEnvironment.scan("MyUserTable")
    //      .select("*")
    //
    //    tableEnvironment.toDataStream(table).print()

  }
}
