package com.gaowj.api.DataStream.jdbc

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
  * Created on 2019-08-15.
  * jdbc
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/src/main/scala/code/book/stream/customsinkandsource/jdbc/scala/JdbcTest.scala
  */
object JdbcDemo {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://gaowj:3306/flinktest"
    val username = "root"
    val password = "gaowj"
    var connection: Connection = null
    var statement: Statement = null

    try {
      Class.forName(driver) // 加载驱动
      connection = DriverManager.getConnection(url, username, password) // 创建连接
      statement = connection.createStatement() // 获得执行语句
      val resultSet: ResultSet = statement.executeQuery("select name,sex,address from student") // 执行查询，获得查询结果集
      while (resultSet.next()) {
        val student: Student = Student(resultSet.getString("name"), resultSet.getString("sex"), resultSet.getString("address"))
        println(student)
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      // 关闭连接，释放资源
      if (connection != null)
        connection.close()
      if (statement != null)
        statement.close()
    }

    connection.close()
  }
}
