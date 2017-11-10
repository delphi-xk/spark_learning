package com.hyzs.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/10/19.
  */
object JdbcTest {

  val conf = new SparkConf().setAppName("JdbcTest")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val df1 = sqlContext.read.format("jdbc").
      options(Map("url"-> "jdbc:hive2://master:10000/default", "dbtable" -> "his_test_1", "user"-> "hive", "password" -> "hive")).load()
    df1.show()
  }


}
