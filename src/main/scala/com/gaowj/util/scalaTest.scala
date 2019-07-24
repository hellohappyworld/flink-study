package com.gaowj.util

import java.text.SimpleDateFormat
import java.util.Date

object scalaTest {
  def main(args: Array[String]): Unit = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = dateFormat.format(now.getTime - 60 * 1000)

    println(date)
  }
}
