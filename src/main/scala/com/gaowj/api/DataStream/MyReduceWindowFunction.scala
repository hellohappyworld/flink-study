package com.gaowj.api.DataStream

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created on 2019-08-14.
  * ProcessWindowFunction
  * original -> http://shiyanjun.cn/archives/1775.html
  */
class MyReduceWindowFunction extends ProcessWindowFunction[((String, String), Long), ((String, String, String, String), Long), Tuple, TimeWindow] {

  def formatTs(startTs: Long) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(new Date(startTs))
  }

  override def process(key: Tuple, context: Context, elements: Iterable[((String, String), Long)], out: Collector[((String, String, String, String), Long)]): Unit = {
    val startTs: Long = context.window.getStart
    val endTs: Long = context.window.getEnd

    for (group <- elements.groupBy(_._1)) {
      val myKey: (String, String) = group._1
      val myValue: Iterable[((String, String), Long)] = group._2
      var count = 0L
      for (elem <- myValue) {
        count += elem._2
      }
      val ip: String = myKey._1
      val timeStr: String = myKey._2
      val outputKey: (String, String, String, String) = (formatTs(startTs), formatTs(endTs), ip, timeStr)
      out.collect((outputKey, count))
    }

  }
}
