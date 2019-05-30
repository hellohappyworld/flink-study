package com.gaowj.api

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.log4j.Logger

/**
  * Created on 2019-05-30.
  * ReduceFunction
  * original -> https://github.com/soniclavier/bigdata-notebook/blob/a708de834fd282ba576f15f87a46f3953695a9ad/flink/src/main/scala/com/vishnu/flink/streaming/queryablestate/QuerybleStateStream.scala
  */

object ReduceFunctionDemo_1 {
  val logger: Logger = Logger.getLogger("ReduceFunctionDemo")

  case class ClimateLog(country: String, state: String, temperature: Float, humidity: Float)

  object ClimateLog {
    def apply(line: String): Option[ClimateLog] = {
      val parts = line.split(",")
      try {
        Some(ClimateLog(parts(0), parts(1), parts(2).toFloat, parts(3).toFloat))
      } catch {
        case e: Exception => {
          logger.warn(s"Unable to parse line $line")
          None
        }
      }
    }
  }

  val reduceFunction = new ReduceFunction[ClimateLog] {
    override def reduce(c1: ClimateLog, c2: ClimateLog): ClimateLog = {
      c1.copy(
        temperature = c1.temperature + c2.temperature,
        humidity = c1.humidity + c2.humidity
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val climateLogStream: DataStream[ClimateLog] = env.socketTextStream("localhost", 9990)
      .flatMap(ClimateLog(_))

    val climateLogAgg: DataStream[ClimateLog] = climateLogStream
      .name("climate-log-agg")
      .keyBy("country", "state")
      .timeWindow(Time.seconds(10))
      .reduce(reduceFunction)

    climateLogAgg.print()

    env.execute("ReduceFunction Demo")
  }


}
