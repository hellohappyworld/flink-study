package com.gaowj.util

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.io.compress.GzipCodec

object scalaTest {
  def main(args: Array[String]): Unit = {

    val list: util.List[String] = FindHdfsPaths.existFiles("/user/flink/backup_file/appsta/2019-07-22--1427/*")
    val arr = list.toArray(new Array[String](list.size))
    arr.foreach(println)
  }
}
