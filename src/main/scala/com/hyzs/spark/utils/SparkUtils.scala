package com.hyzs.spark.utils


import java.util.concurrent.atomic.AtomicReference

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SizeEstimator



/**
  * Created by Administrator on 2018/1/24.
  */
object SparkUtils {
  val conf: SparkConf = new SparkConf().setAppName("JDProcess")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = HiveContextUtil.getOrCreate(sc)
  val hdConf: Configuration = sc.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(hdConf)

  val partitionNums: Int = Option(sqlContext.getConf("spark.sql.shuffle.partitions")).getOrElse("200").toInt
  val warehouseDir = "/user/hive/warehouse/"
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  // NOTE: not serializable
  //  ObjectMapper.registerModule(DefaultScalaModule)
  val broadMapper: Broadcast[ObjectMapper] = sc.broadcast(mapper)

  def processNull(df: DataFrame): DataFrame = {
    df.na.fill(0.0)
      .na.fill("0.0")
      .na.replace("*", Map("" -> "0.0",
      "null" -> "0.0", "NULL" -> "0.0",
      "-9999"->"0"))
      .na.replace("*", Map(-9999 -> 0))
  }


  def checkHDFileExist(filePath: String): Boolean = {
    val path = new Path(filePath)
    fs.exists(path)
  }

  def dropHDFiles(filePath: String): Unit = {
    val path = new Path(filePath)
    fs.delete(path, true)
  }

  def mkHDdir(dirPath:String): Unit = {
    val path = new Path(dirPath)
    if(!fs.exists(path))
      fs.mkdirs(path)
    else if(fs.exists(path)&&fs.getFileStatus(path).isFile){
      fs.delete(path,false)
      fs.mkdirs(path)
    }
  }
  def moveHDFile(oldFilePath:String, newFilePath:String): Unit = {
    val path = new Path(oldFilePath)
    val newPath = new Path(newFilePath)
    FileUtil.copy(fs, path, fs, newPath, false, hdConf)
  }

  def copyMergeHDFiles(srcFileDir:String, dstFile:String): Unit = {
    val srcDir = new Path(srcFileDir)
    val file = new Path(dstFile)
    fs.delete(file, true)
    FileUtil.copyMerge(fs, srcDir, fs, file, false, hdConf, null)
  }

  def saveTable(df: DataFrame, tableName:String, dbName:String="hyzs"): Unit = {
    sqlContext.sql(s"drop table if exists $dbName.$tableName")
    var path = ""
    if(dbName == "default") path = s"$warehouseDir$tableName"
    else path = s"$warehouseDir$dbName.db/$tableName"

    if(checkHDFileExist(path))dropHDFiles(path)
    df.write
      .option("path",path)
      .saveAsTable(s"$dbName.$tableName")
  }

  def createDFfromCsv(path: String, delimiter: String = "\\t"): DataFrame = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter, -1))
      .filter(row => row.length == cols.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  // filter malformed data
  def createDFfromRawCsv(header: Array[String], path: String, delimiter: String = ","): DataFrame = {
    val data = sc.textFile(path)
    val cols = header.map( col => StructField(col, StringType))
    val rows = data.map( lines => lines.split(delimiter, -1))
      .filter(row => row.length == cols.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  def createDFfromSeparateFile(headerPath: String, dataPath: String,
                               headerSplitter: String=",", dataSplitter: String="\\t"): DataFrame = {
    //println(s"header path: ${headerPath}, data path: ${dataPath}")
    val header = sc.textFile(headerPath)
    val fields = header.first().split(headerSplitter)
    createDFfromRawCsv(fields, dataPath, dataSplitter)
  }

  def createDFfromBadFile(headerPath: String, dataPath: String,
                          headerSplitter: String=",", dataSplitter: String="\\t"): DataFrame = {
    val headerFile = sc.textFile(headerPath)
    val dataFile = sc.textFile(dataPath)
    val header = headerFile.first().split(headerSplitter)
      .map( col => StructField(col, StringType))
    val rows = dataFile.filter(row => !row.isEmpty)
      .map( (row:String) => {
        val arrs = row.split("\\t", -1)
        arrs(0) +: arrs(1) +: arrs(2).split(",",-1)
      })
      .filter( arr => arr.length <= header.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(header)
    sqlContext.createDataFrame(rows, struct)
  }

  def estimator[T](rdd: RDD[T]): Long = {
    SizeEstimator.estimate(rdd)
  }

}
