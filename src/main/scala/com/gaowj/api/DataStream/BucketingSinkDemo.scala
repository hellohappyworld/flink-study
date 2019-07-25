package com.gaowj.api.DataStream

import java.text.SimpleDateFormat
import java.util.Date

import com.gaowj.util.{ActiveNNAdd, KafkaConsumerDemo}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.fs.bucketing.{BasePathBucketer, BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.fs.{Clock, StringWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}

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
    val addNN: ActiveNNAdd = new ActiveNNAdd
    val adress: String = addNN.getNameNodeAdress
    val checkpointPath = adress + "/user/flink/flink-checkpoints"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(45000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend(checkpointPath))
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)


    val gzipCodec: GzipCodec = new GzipCodec
    // 添加Hadoop配置内容
    val conf = new Configuration()
    conf.setBoolean("mapred.compress.map.output", true)
    conf.setClass("mapred.map.output.compression.codec", classOf[GzipCodec], classOf[CompressionCodec])
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
    conf.setClass("mapreduce.output.fileoutputformat.compress.codec", classOf[GzipCodec], classOf[CompressionCodec])
    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK")

    //    conf.setString("mapreduce.output.fileoutputformat.compress", "true")
    //    conf.setString("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
    //    conf.setString("mapreduce.output.fileoutputformat.compress.codec", gzipCodec.getClass.getName)
    //    conf.setString("mapreduce.map.output.compress", "true")
    //    conf.setString("mapreduce.map.output.compress.codec", gzipCodec.getClass.getName)

    //    config.setBoolean("mapreduce.map.output.compress", true)
    //    config.setClass("mapreduce.map.output.compress.codec", gzipCodec.getClass)
    //    config.setBoolean("mapreduce.output.fileoutputformat.compress", true)
    //    config.setClass("mapreduce.output.fileoutputformat.compress.codec", gzipCodec.getClass)


    val sourceDs: DataStream[String] = KafkaConsumerDemo.getKafkaData(env)

    //    val sink: BucketingSink[String] = new BucketingSink("D:\\workStation\\Test\\logParse\\appsta\\src20190723")
    val sink: BucketingSink[String] = new BucketingSink(s"${adress}/user/flink/backup_file/test")
    //    sink.setBucketer(new DayBasePathBucketer)
    sink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHHmm"))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchRolloverInterval(60 * 1000)
    sink.setFSConfig(conf)

    sourceDs.addSink(sink).setParallelism(1)

    env.execute("BucketingSinkDemo")
  }

  class DayBasePathBucketer extends BasePathBucketer[String] {
    override def getBucketPath(clock: Clock, basePath: Path, element: String): Path = {
      new Path(basePath.toString)
    }
  }

}


