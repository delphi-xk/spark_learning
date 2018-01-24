package com.hyzs.spark.utils


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2018/1/24.
  */
object SparkUtils {
  val warehouseDir = "/hyzs/warehouse/hyzs.db/"
  val conf = new SparkConf().setAppName("ScalaSpark")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val hdConf = sc.hadoopConfiguration
  val fs = FileSystem.get(hdConf)
  val defaultDb = "hyzs"

  def checkHDFileExist(filePath: String): Boolean = {
    val path = new Path(filePath)
    fs.exists(path)
  }

  def dropHDFiles(filePath: String): Unit = {
    val path = new Path(filePath)
    fs.delete(path, true)
  }
  def saveTable(df: DataFrame, tableName:String): Unit = {
    sqlContext.sql(s"drop table if exists $defaultDb.$tableName")
    val path = s"$warehouseDir$tableName"
    if(checkHDFileExist(path))dropHDFiles(path)
    //println("xkqyj going to save")
    df.write
      //.format("orc")
      .option("path",path)
      .saveAsTable(s"hyzs.$tableName")
  }


}
