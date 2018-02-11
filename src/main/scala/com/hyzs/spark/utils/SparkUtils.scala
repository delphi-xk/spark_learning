package com.hyzs.spark.utils


import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext



/**
  * Created by Administrator on 2018/1/24.
  */
object SparkUtils {
  val conf: SparkConf = new SparkConf().setAppName("DataProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val hdConf: Configuration = sc.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(hdConf)

  val partitionNums: Int = Option(sqlContext.getConf("spark.sql.shuffle.partitions")).getOrElse("200").toInt
  val warehouseDir = "/hyzs/warehouse/hyzs.db/"
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  // NOTE: not serializable
  //  .registerModule(DefaultScalaModule)
  val broadMapper: Broadcast[ObjectMapper] = sc.broadcast(mapper)


  def checkHDFileExist(filePath: String): Boolean = {
    val path = new Path(filePath)
    fs.exists(path)
  }

  def dropHDFiles(filePath: String): Unit = {
    val path = new Path(filePath)
    fs.delete(path, true)
  }
  def saveTable(df: DataFrame, tableName:String, dbName:String="hyzs"): Unit = {
    sqlContext.sql(s"drop table if exists $dbName.$tableName")
    val path = s"$warehouseDir$tableName"
    if(checkHDFileExist(path))dropHDFiles(path)
    df.write
      .option("path",path)
      .saveAsTable(s"$dbName.$tableName")
  }



}
