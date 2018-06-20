package com.hyzs.spark.utils


import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2018/6/20.
  */
object SparkUtilsLocal {
  val spark:SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("Spark local process")
    .getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext
  val sc:SparkContext = spark.sparkContext
  val conf:SparkConf = sc.getConf

  def processNull(df: Dataset[Row]): Dataset[Row] = {
    df.na.fill(0.0)
      .na.fill("0.0")
      .na.replace("*", Map("" -> "0.0",
      "null" -> "0.0", "NULL" -> "0.0",
      "-9999"->"0"))
      .na.replace("*", Map(-9999 -> 0))
  }

  // process empty value for label generation
  def processNullByCols(df: Dataset[Row], cols: Seq[String]): Dataset[Row] = {
    df.na.fill("0.0")
      .na.fill(0.0)
      .na.replace(cols, Map("" -> "0.0","null" -> "0.0", "NULL" -> "0.0", -9999 -> 0))
  }

  def saveTable(df: Dataset[Row], tableName:String, dbName:String = "default"): Unit = {
    spark.sql(s"drop table if exists $dbName.$tableName")
    df.write.saveAsTable(s"$dbName.$tableName")
  }


  def readCsv(path:String, delimiter:String): Dataset[Row] = {
    spark.read
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(path)

  }


}


