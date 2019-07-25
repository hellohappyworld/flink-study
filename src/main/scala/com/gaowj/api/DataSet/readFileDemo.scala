package com.gaowj.api.DataSet

import java.util

import com.gaowj.util.FindHdfsPaths
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path

/**
  * Created by gaowj on 2019-07-25.
  * Function：测试Fink读取多文件和嵌套文件数据
  */
object readFileDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val paraments = new Configuration()
    paraments.setBoolean("recursive.file.enumeration", true)

    //读取嵌套文件
    /*
    //    val sourceDs: DataSet[String] = env.readTextFile("hdfs://10.90.92.148:8020/user/flink/backup_file/appsta/2019-07-22--1427")
    val sourceDs: DataSet[String] = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190723")
      .withParameters(paraments)
    sourceDs.print()
    */

    //读取嵌套文件
    //    val list: util.List[String] = FindHdfsPaths.existFiles("/user/flink/backup_file/appsta/2019-07-22--1427/*")
    val list: util.List[String] = FindHdfsPaths.existFiles(args(0))
    val paths = list.toArray(new Array[String](list.size))
    //    val format: TextInputFormat = new TextInputFormat(new Path("hdfs://10.90.92.148:8020/user/flink/backup_file/appsta/2019-07-22--1427"))
    val format: TextInputFormat = new TextInputFormat(new Path(args(0)))
    //    format.setFilePaths("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\readFileDemo") // 该文件夹下有多个文件
    //    format.setFilePaths("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\readFileDemo",
    //      "D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\media_f")
    //    val paths = Array("D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\media_f",
    //      "D:\\workStation\\ProjectStation\\flink-study\\src\\resources\\readFileDemo")
    format.setFilePaths(paths: _*)
    //    format.setCharsetName("UTF-8")
    //    format.supportsMultiPaths()
    val sourceDs: DataSet[String] = env.createInput(format).withParameters(paraments)
    //    sourceDs.print()

    println(sourceDs.count())
  }
}

