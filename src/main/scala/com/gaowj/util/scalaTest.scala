package com.gaowj.util

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.io.compress.GzipCodec

object scalaTest {
  def main(args: Array[String]): Unit = {
    val str = "119.188.101.187"
    val arr: Array[String] = str.split("\\.")
    arr.foreach(println)
  }
}
