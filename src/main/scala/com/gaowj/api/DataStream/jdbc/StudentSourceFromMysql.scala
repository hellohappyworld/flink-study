package com.gaowj.api.DataStream.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * Created on 2019-08-16.
  * RichSourceFunction
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/src/main/scala/code/book/stream/customsinkandsource/jdbc/scala/StudentSourceFromMysql.scala
  */
class StudentSourceFromMysql extends RichSourceFunction[Student] {
  private var connection: Connection = null
  private var ps: PreparedStatement = null

  /**
    * open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://gaowj:3306/flinktest"
    val username = "root"
    val password = "gaowj"
    Class.forName(driver) // 加载驱动
    connection = DriverManager.getConnection(url, username, password) // 创建连接
    val sql = "select name,sex,address from student;"
    ps = connection.prepareStatement(sql)
  }

  /**
    * DataStream调用一次run()方法用来获取数据
    *
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    try {
      // 执行查询，封装数据
      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()) {
        val student: Student = Student(resultSet.getString("name"), resultSet.getString("sex"), resultSet.getString("address"))
        sourceContext.collect(student)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  override def cancel(): Unit = {}

  /**
    * 程序执行完毕进行关闭连接和释放资源的动作
    */
  override def close(): Unit = {
    super.close()
    if (connection != null)
      connection.close()
    if (ps != null)
      ps.close()
  }
}














