package com.gaowj.api.DataStream.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Created on 2019-08-15.
  * RichSinkFunction/mysql
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/src/main/scala/code/book/stream/customsinkandsource/jdbc/scala/StudentSinkToMysql.scala
  */
class StudentSinkToMysql extends RichSinkFunction[Student] {
  private var connection: Connection = null
  private var ps: PreparedStatement = null

  /**
    * 该方法中建立连接，这样不用每次invoke的时候都要简历连接和释放连接
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
    val sql = "insert into student(name,sex,address) values(?,?,?);"
    ps = connection.prepareStatement(sql)
  }

  /**
    * 每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
    *
    * @param stu
    */
  override def invoke(stu: Student): Unit = {
    try {
      // 执行插入操作
      ps.setString(1, stu.name)
      ps.setString(3, stu.address)
      ps.setString(2, stu.name)
      ps.executeUpdate()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  /**
    * 程序执行完毕时进行关闭连接和释放资源的动作
    */
  override def close(): Unit = {
    super.close()
    // 关闭连接和释放资源
    if (connection != null)
      connection.close()
    if (ps != null)
      ps.close()
  }

}
