package com.gaowj.api.DataStream

import java.text.SimpleDateFormat
import java.util.Date

import com.gaowj.util.KafkaConsumerDemo
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Created by gaowj on 2019-07-23.
  * Functions:StreamingFileSink
  */
object StreamingFileSinkDemo {

  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = dateFormat.format(now)
    return date
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    sourceDs.writeUsingOutputFormat(new TextOutputFormat[String](new Path(s"D:\\workStation\\Test\\logParse\\appsta\\src20190723\\${nowDate()}.txt")))


    //    val sink = StreamingFileSink
    //      .forRowFormat(new Path("D:\\workStation\\Test\\logParse\\appsta\\src20190723"), new SimpleStringEncoder[String]("UTF-8"))
    //      //      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd--HHmm"))
    //      .withBucketAssigner(new otherBucketAssigner)
    //      .withBucketCheckInterval(1000)
    //      .build()
    //
    //    sourceDs.addSink(sink).setParallelism(1)


    env.execute("StreamingFileSink")
  }

  //  class otherBucketAssigner extends BucketAssigner[String, String] {
  //    override def getBucketId(in: String, context: BucketAssigner.Context): String = null
  //
  //    override def getSerializer: SimpleVersionedSerializer[String] = null
  //  }


}
