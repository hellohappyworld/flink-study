package com.gaowj.api.DataStream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * Created by gaowj on 2019-05-29
  * ConnectedStreams.
  *
  */
object ConnectedStreamsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1: DataStream[String] = env.readTextFile("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv1")
    val ds2: DataStream[String] = env.readTextFile("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\filecsv2")

    val connectedDs: ConnectedStreams[String, String] = ds1.connect(ds2)

    val mapDs: DataStream[String] = connectedDs.map(line1 => {
      line1
    }, line2 => {
      line2
    })
    //      3,33,4aaa
    //      3,33,3aaa
    //      2,22,2bbb
    //      1,11,1bbb
    //      3,33,3bbb
    //      1,11,1aaa
    //      2,22,2aaa
    mapDs.print()

    val flatMapDs: DataStream[String] = connectedDs.flatMap(line1 => {
      line1.split(",")
    }, line2 => {
      line2.split(",")
    })
    //    3
    //    33
    //    4aaa
    //      3
    //    33
    //    3bbb
    //      3
    //    33
    //    3aaa
    //      2
    //    22
    //    2bbb
    //      1
    //    11
    //    1bbb
    //      1
    //    11
    //    1aaa
    //      2
    //    22
    //    2aaa
    flatMapDs.print()

    env.execute("ConnectedStreams demo")
  }
}
