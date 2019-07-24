package com.gaowj.api.DataStream

import java.text.SimpleDateFormat
import java.util.Date

import com.gaowj.util.{ActiveNNAdd, KafkaConsumerDemo}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.fs.{Clock, StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BasePathBucketer, BucketingSink, DateTimeBucketer}
import org.apache.hadoop.fs.Path

/**
  * Created by gaowj on 2019-07-23.
  * Functions:BucketingSink
  */
object BucketingSinkDemo {
  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = dateFormat.format(now)
    return date
  }

  def main(args: Array[String]): Unit = {
    //    val addNN: ActiveNNAdd = new ActiveNNAdd
    //    val adress: String = addNN.getNameNodeAdress
    //    val checkpointPath = adress + "/user/flink/flink-checkpoints"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.enableCheckpointing(45000)
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    env.setStateBackend(new FsStateBackend(checkpointPath))
    //    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    val sink: BucketingSink[String] = new BucketingSink("D:\\workStation\\Test\\logParse\\appsta\\src20190723")
    //    val sink: BucketingSink[String] = new BucketingSink(s"${adress}/user/flink/backup_file/test")
    sink.setBucketer(new DayBasePathBucketer)
    sink.setWriter(new StringWriter[String]())
    sink.setBatchRolloverInterval(60 * 1000)
    sink.setInProgressPrefix(nowDate())
    sink.setPendingPrefix(nowDate())
    sink.setPartPrefix(nowDate())

    sourceDs.addSink(sink).setParallelism(1)

    env.execute("BucketingSinkDemo")
  }

  class DayBasePathBucketer extends BasePathBucketer[String] {
    override def getBucketPath(clock: Clock, basePath: Path, element: String): Path = {
      new Path(basePath.toString)
    }
  }

}


