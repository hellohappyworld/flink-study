package com.gaowj.api.DataStream.jdbc

/**
  * 用于存储数据库中的数据，作为bean使用
  * original -> https://github.com/liguohua-bigdata/simple-flink/blob/master/src/main/scala/code/book/stream/customsinkandsource/jdbc/scala/Student.scala
  * @param name
  * @param sex
  * @param address
  */
case class Student(name: String, sex: String, address: String)
