package com.gaowj.api.TableSQL

import java.util

import com.gaowj.util.FindHdfsPaths
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, JoinedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment


/**
  * Created by gaowj on 2019-04-24.
  * Functions:交集和差集
  */
object InterAndMinus {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    val countds: DataSet[String] = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190712\\standalone").filter(line => line.contains("ERROR") && line.contains("GET /appsta.js"))
    //    println(countds.count())

    /**
      * 合并数据
      */
    /*//    var newAllData = env.readTextFile("D:\\workStation\\Test\\logParse\\vplayer\\src20190520\\201905201110.sta")
    var newAllData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190704\\access_dig.201907040930")
    for (i <- 1 to 9) {
      //      val inpath: String = s"D:\\workStation\\Test\\logParse\\vplayer\\src20190520\\20190520111$i.sta"
      val inpath: String = s"D:\\workStation\\Test\\logParse\\appsta\\src20190704\\access_dig.20190704093$i"
      val inData = env.readTextFile(inpath)
      newAllData = newAllData.union(inData)
    }
    //    newAllData.writeAsText("D:\\workStation\\Test\\logParse\\vplayer\\src20190520\\old_f").setParallelism(1)
    newAllData.writeAsText("D:\\workStation\\Test\\logParse\\appsta\\src20190704\\src_f").setParallelism(1)
    env.execute("1")*/

    /**
      * 数据交集
      */
    //     val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //     var newAllData = env.readTextFile("D:\\workStation\\Test\\logParse\\vplayer\\src20190520\\new_f")
    //
    //     val value1 = newAllData
    //     //      .map(line => {
    //     //        val arr: Array[String] = line.split("\t")
    //     //        arr(0) + "--" + arr(1)
    //     //      })
    //     println("flink-->" + newAllData.count())
    //     val oldData = env.readTextFile("D:\\workStation\\Test\\logParse\\vplayer\\src20190520\\old_f")
    //     //      .map(line => {
    //     //        val arr: Array[String] = line.split("\t")
    //     //        arr(0) + "--" + arr(1)
    //     //      })
    //     println("python-->" + oldData.count())
    //     val table_newData: Table = tableEnv.fromDataSet(value1)
    //     val table_oldData: Table = tableEnv.fromDataSet(oldData)
    //
    //     val table_inter: Table = table_newData.intersectAll(table_oldData)
    //     val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    //     print(interData.count())

    /**
      * 差集
      */
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val paraments = new Configuration()
    paraments.setBoolean("recursive.file.enumeration", true)

    val flink_list: util.List[String] = FindHdfsPaths.existFiles(args(0))
    val flink_paths = flink_list.toArray(new Array[String](flink_list.size))
    val flink_format: TextInputFormat = new TextInputFormat(new Path(args(0)))
    flink_format.setFilePaths(flink_paths: _*)
    val flinkDs: DataSet[String] = env.createInput(flink_format).withParameters(paraments)
      .map(line => {
        val arr: Array[String] = line.split("\t")
        arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10) + "\t" + arr(11) + "\t" + arr(12)
      })
    println("------flink count-------" + flinkDs.count())
    val py_list: util.List[String] = FindHdfsPaths.existFiles(args(1))
    val py_paths = py_list.toArray(new Array[String](py_list.size))
    val py_format: TextInputFormat = new TextInputFormat(new Path(args(1)))
    py_format.setFilePaths(py_paths: _*)
    val pyDs: DataSet[String] = env.createInput(py_format).withParameters(paraments)
      .map(line => {
        val arr: Array[String] = line.split("\t")
        arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10) + "\t" + arr(11) + "\t" + arr(12)
      })
    println("------py count-------" + pyDs.count())
    //*****交集*****
    val table_flinkDs: Table = tableEnv.fromDataSet(flinkDs)
    val table_pyDs: Table = tableEnv.fromDataSet(pyDs)
    val table_inter: Table = table_flinkDs.intersectAll(table_pyDs)
    val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    print("------inter count-------" + interData.count())
    //    print("flink:" + flinkDs.count() + "py" + pyDs.count() + "inter:" + interData.count())
    //******差集********
    //    val minusTable: Table = table_pyDs.minusAll(table_inter)
    //    val minusTable: Table = table_pyDs.minusAll(table_inter)
    //    val minusTable: Table = table_pyDs.minusAll(table_inter)
    //    val minuxData: DataSet[String] = tableEnv.toDataSet[String](minusTable)
    //    minuxData.print()
    //    //    //    println(minuxData.count())
    //    minuxData.writeAsText("D:\\workStation\\Test\\logParse\\appsta\\src20190805\\linshi\\minus_py", WriteMode.OVERWRITE).setParallelism(1)
    //    env.execute("chaji")

    /**
      * 查找数据
      */
    //    var newAllData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190517\\src_f")
    //    newAllData.filter(line => line.contains("122.96.40.88") && line.contains("android") && line.contains("page") && line.contains("950810a2a9a0cf4a1bb43621462d24c4")).print()

    /**
      * 数据交集 2019-05-08/1650.sta
      */
    //    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    var newAllData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190508\\app_news_f1")
    //    //    for (i <- 1 to 9) {
    //    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\appsta\\src20190508\\20190508165$i.sta"
    //    //      val inData = env.readTextFile(inpath)
    //    //      newAllData = newAllData.union(inData)
    //    //    }
    //    //    newAllData.writeAsText("D:\\workStation\\Test\\logParse\\appsta\\src20190508\\app_old_f1").setParallelism(1)
    //    //    env.execute("1")
    //    //
    //    val value1 = newAllData.map(line => {
    //      val arr: Array[String] = line.split("\t")
    //
    //      arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10) + "\t" + arr(11) + "\t" + arr(12)
    //    })
    //    //    println(newAllData.count()) // 86296
    //    val oldData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190508\\app_old_f1").map(line => {
    //      val arr: Array[String] = line.split("\t")
    //
    //      arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10) + "\t" + arr(11) + "\t" + arr(12)
    //    })
    //    //    println(oldData.count()) // 86289
    //    val table_newData: Table = tableEnv.fromDataSet(value1)
    //    val table_oldData: Table = tableEnv.fromDataSet(oldData)
    //    //
    //    val table_inter: Table = table_newData.intersectAll(table_oldData)
    //    val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    //    println(interData.count()) //86289
    //    tableEnv.toDataSet[String](table_newData.minusAll(table_inter))
    //      .writeAsText("D:\\workStation\\Test\\logParse\\appsta\\20190508\\new_minus_f", WriteMode.OVERWRITE).setParallelism(1)
    //    tableEnv.toDataSet[String](table_oldData.minusAll(table_inter))
    //      .writeAsText("D:\\workStation\\Test\\logParse\\appsta\\20190508\\old_minus_f", WriteMode.OVERWRITE).setParallelism(1)
    //    //    val interData = tableEnv.toDataSet[String](table_inter)
    //    //    interData.print()
    //    //    println(interData.count()) // 520915
    //    env.execute("")

    /**
      * 源数据中查找某条数据
      */
    //    var kafkaDStream = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src\\access_dig.201905071640")
    //    for (i <- 1 to 9) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\appsta\\src\\access_dig.20190507164$i"
    //      val inData = env.readTextFile(inpath)
    //      kafkaDStream = kafkaDStream.union(inData)
    //    }
    //    //    val filData = kafkaDStream.filter(line => line.contains("ipad_8.4.1") && line.contains("推荐"))
    //    val filData = kafkaDStream.filter(line => line.contains("113.2.64.144"))
    //    filData.print()
    //    env.execute("2")


    /**
      * 取差集
      */
    //    val newData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\src20190508\\app_news_f2").map(line => {
    //      val arr: Array[String] = line.split("\t")
    //
    //      arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10)
    //    })
    //    val oldData = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\old_app_new_f").map(line => {
    //      val arr: Array[String] = line.split("\t")
    //
    //      arr(0) + "\t" + arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7) + "\t" + arr(8) + "\t" + arr(10)
    //    })
    //    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    val table_newData: Table = tableEnv.fromDataSet(newData)
    //    val table_oldData: Table = tableEnv.fromDataSet(oldData)
    //    val table_inter: Table = table_oldData.intersectAll(table_newData)
    //    tableEnv.toDataSet[String](table_newData.minusAll(table_inter)).writeAsText("D:\\workStation\\Test\\logParse\\appsta\\new_minus_f", WriteMode.OVERWRITE).setParallelism(1)
    //    tableEnv.toDataSet[String](table_oldData.minusAll(table_inter)).writeAsText("D:\\workStation\\Test\\logParse\\appsta\\old_minus_f", WriteMode.OVERWRITE).setParallelism(1)
    //    //    val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    //    //    println(interData.count()) //
    //    env.execute("1")


    /**
      * python代码清洗与老清洗逻辑对比
      */
    //    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    var oldData1 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1000.1555725604943.log")
    //    val oldData2 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1001.1555725664614.log")
    //    val oldData3 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1002.1555725724356.log")
    //    val oldData4 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1003.1555725782719.log")
    //    val oldData5 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1004.1555725849712.log")
    //    val oldData6 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1005.1555725912373.log")
    //    val oldData7 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1006.1555725961136.log")
    //    val oldData8 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1007.1555726026879.log")
    //    val oldData9 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1008.1555726094793.log")
    //    val oldData10 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1009.1555726140705.log")
    //    val oldData11 = env.readTextFile("hdfs://10.90.7.168:8020/user/source/app_newsapp/20190420/1010.1555726206694.log")
    //    val oldAllData: DataSet[String] = oldData1.union(oldData2).union(oldData3).union(oldData4).union(oldData5).union(oldData6).union(oldData7).union(oldData8).union(oldData9).union(oldData10).union(oldData11)
    //    //    println(oldAllData.count()) // 4974664
    //    var newData: DataSet[String] = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-07\\_part-0-0.pending")
    //    for (i <- 1 to 15) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-07\\_part-0-$i.pending"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      newData = newData.union(inData)
    //    }
    //    //    println(newData.count()) // 4959972
    //
    //    //        val newData1 = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-07/_part-0-0.pending")
    //    //        val newData2 = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-07/_part-0-1.pending")
    //    //        val newData = newData1.union(newData2)
    //    //    println(newData.count()) //
    //    val table_oldData: Table = tableEnv.fromDataSet(oldAllData)
    //    val table_newData: Table = tableEnv.fromDataSet(newData)
    //    val table_inter: Table = table_oldData.intersectAll(table_newData)
    //    val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    //    println(interData.count()) //


    /**
      * 数据交集
      */
    //    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    val newData: DataSet[String] = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\all")
    //    val oldData: DataSet[String] = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\1200.sta")
    //    val table_new: Table = tableEnv.fromDataSet(newData)
    //    val table_old: Table = tableEnv.fromDataSet(newData)
    //
    //    val table_inter: Table = table_new.intersectAll(table_old)
    //    val interData: DataSet[String] = tableEnv.toDataSet[String](table_inter)
    //    interData.print()
    //
    //    //    val sink: CsvTableSink = new CsvTableSink("D:\\workStation\\Test\\logParse\\appsta\\jiaoji", fieldDelim = "\t")
    //    //result.writeToSink(sink)

    /**
      * 整合数据
      */
    //    val part1 = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-05/_part-0-0.pending")
    //    val part2 = env.readTextFile("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-05-05/_part-0-1.pending")
    //    val all: DataStream[String] = part1.union(part2)
    //    all.writeAsText("D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\all").setParallelism(1)
    //    env.execute("union datastream")


    /**
      * 统计appsta新清洗代码流结果数据
      */
    //    var inAllData: DataSet[String] = env.readTextFile(appsta_app_news_f)
    //    for (i <- 1 to 2) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\appsta\\app_news_f\\2019-04-30\\_part-0-$i.pending"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // 402450

    /**
      * 统计appsta老清洗代码流结果数据
      */
    //    var inAllData: DataSet[String] = env.readTextFile(old_appsta_app_news_f)
    //    println(inAllData.count()) // 402479


    /**
      * 统计vplayer新清洗代码流结果数据：
      */
    //    var inAllData: DataSet[String] = env.readTextFile(vplayer_vplay_f)
    //    //        for (i <- 18 to 26) { //这是统计的2019-04-25中午12:00到12:09（包括）的数据条数
    //    for (i <- 18 to 46) {
    //      val inpath: String = s"hdfs://10.80.15.158:8020/user/tongji/vplayerlog/vplay_f/2019-04-25/part-0-7$i"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // vdur_f 377  vplay_f 1102188
    /**
      * 统计vplayer新清洗代码流结果数据：vdur_f
      */
    //    var inAllData: DataSet[String] = env.readTextFile(old_vplayer_vplay_f)
    //    //        for (i <- 18 to 26) { //这是统计的2019-04-25中午12:00到12:09（包括）的数据条数
    //    for (i <- 1 to 9) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\vplayer\\vplay_f/120$i.sta.gz"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    for (i <- 10 to 29) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\vplayer\\vplay_f/12$i.sta.gz"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // vdur_f374        vplay_f 1095510


    /**
      * 统计media新清洗代码流结果数据：media_f
      */
    //    var inAllData: DataSet[String] = env.readTextFile(media_f0)
    //    //        for (i <- 18 to 26) { //这是统计的2019-04-25中午12:00到12:09（包括）的数据条数
    //    for (i <- 17 to 25) { //这是2019-04-25中午12:20到12:29的数据
    //      val inpath: String = s"hdfs://10.80.15.158:8020/user/tongji/medialog/media_wap_f/2019-04-25/part-0-7$i"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // media_f 97343      media_wap_f 12001
    /**
      * 统计media老清洗代码流结果数据：media_f
      */
    //    var inAllData: DataSet[String] = env.readTextFile(old_media_f_inPath0)
    //    for (i <- 1 to 9) {
    //      val inpath: String = s"D:\\workStation\\Test\\logParse\\media\\old_media_wap_f/122$i.sta.gz"
    //      //      println(inpath)
    //      val inData: DataSet[String] = env.readTextFile(inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // media_f 96466         media_wap_f 11912

    /**
      * 统计media新清洗代码结果数据：media_f
      */
    //    //    var inAllData: DataSet[String] = env.readTextFile(media_f_inPath0)
    //    var inData0: DataSet[String] = env.readTextFile(media_f_inPath0)
    //    var inData1: DataSet[String] = env.readTextFile(media_f_inPath1)
    //    val inAllData: DataSet[String] = inData0.union(inData1)
    //    //    for (i <- 1 to 35) {
    //    //      //      println(i)
    //    //      val media_inpath: String = s"hdfs://10.80.15.158:8020/user/tongji/medialog/verify/media_f/2019-04-23/_part-0-$i.pending"
    //    //      //      println(media_inpath)
    //    //      val inData: DataSet[String] = env.readTextFile(media_inpath)
    //    //      inAllData = inAllData.union(inData)
    //    //    }
    //    println(inAllData.count()) // 53828
    /**
      * 统计media老清洗代码结果数据：media_f
      */
    //    var inAllData: DataSet[String] = env.readTextFile(old_media_f_inPath0)
    //    for (i <- 1 to 9) {
    //      val media_inpath: String = s"D:\\workStation\\Test\\logParse\\old_media_f/000$i.sta.gz"
    //      val inData: DataSet[String] = env.readTextFile(media_inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // 53828
    // --------------------------------------------------------------------------------------
    /**
      * 统计media新清洗代码结果数据：media_wap_f
      */
    //    val inData0: DataSet[String] = env.readTextFile(media_wap_f_inPath0)
    //    println(inData0.count()) // 554
    //    var inAllData: DataSet[String] = env.readTextFile(media_wap_f_inPath0)
    //    //    println(inAllData.count())
    //    for (i <- 1 to 35) {
    //      //      println(i)
    //      val media_wap_inpath: String = s"hdfs://10.80.15.158:8020/user/tongji/medialog/verify/media_wap_f/2019-04-23/_part-0-$i.pending"
    //      //      println(media_inpath)
    //      val inData: DataSet[String] = env.readTextFile(media_wap_inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count())
    /**
      * 统计media老清洗代码结果数据：media_wap_f
      */
    //    var inAllData: DataSet[String] = env.readTextFile(old_media_wap_f_inPath0)
    //    for (i <- 1 to 9) {
    //      val media_inpath: String = s"D:\\workStation\\Test\\logParse\\old_media_wap_f/000$i.sta.gz"
    //      val inData: DataSet[String] = env.readTextFile(media_inpath)
    //      inAllData = inAllData.union(inData)
    //    }
    //    println(inAllData.count()) // 554
  }
}
