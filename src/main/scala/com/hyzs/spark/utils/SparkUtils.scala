package com.hyzs.spark.utils


import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SizeEstimator


/**
  * Created by Administrator on 2018/1/24.
  */
object SparkUtils {
  val warehouseDir = "/user/hive/warehouse/"
  val spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    //.config("spark.sql.warehouse.dir", warehouseDir)
    .enableHiveSupport()
    .getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext
  val sc:SparkContext = spark.sparkContext
  val conf:SparkConf = sc.getConf
  val hdConf: Configuration = sc.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(hdConf)

  val partitionNums: Int = conf.getOption("spark.sql.shuffle.partitions").getOrElse("200").toInt
  val invalidRowPath = "/hyzs/invalidRows/"
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  // NOTE: not serializable, cannot initialize class.
  //mapper.registerModule(DefaultScalaModule)
  val broadMapper: Broadcast[ObjectMapper] = sc.broadcast(mapper)

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

  def saveTable(df: Dataset[Row], tableName:String, dbName:String = "default"): Unit = {

    spark.sql(s"drop table if exists $dbName.$tableName")
    var path = ""
    if(dbName != "default"){
      path = s"$warehouseDir$dbName.db/$tableName"
    }
    else{
      path = s"$warehouseDir$tableName"
    }
    if(checkHDFileExist(path))dropHDFiles(path)
    df.write
      .saveAsTable(s"$dbName.$tableName")
  }

  def createDFfromCsv(path: String, delimiter: String = "\\t"): Dataset[Row] = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter, -1))
      .filter(row => row.length == cols.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    spark.createDataFrame(rows, struct)
  }

  // filter malformed data
  def createDFfromRawCsv(header: Array[String], path: String, delimiter: String = ","): Dataset[Row] = {
    val data = sc.textFile(path)
    val cols = header.map( col => StructField(col, StringType))
    val rows = data.map( lines => lines.split(delimiter, -1))
      .filter(row => row.length == cols.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    spark.createDataFrame(rows, struct)
  }

  def createDFfromSeparateFile(headerPath: String, dataPath: String,
                               headerSplitter: String=",", dataSplitter: String="\\t"): Dataset[Row] = {
    //println(s"header path: ${headerPath}, data path: ${dataPath}")
    val header = sc.textFile(headerPath)
    val fields = header.first().split(headerSplitter)
    createDFfromRawCsv(fields, dataPath, dataSplitter)
  }

  def createDFfromBadFile(headerPath: String, dataPath: String,
                          headerSplitter: String=",", dataSplitter: String="\\t", logPath:String): Dataset[Row] = {

    val headerFile = sc.textFile(headerPath)
    val dataFile = sc.textFile(dataPath)
    val header = headerFile.first().split(headerSplitter)
      .map( col => StructField(col, StringType))
    val rows = dataFile.filter(row => !row.isEmpty)
      .map( (row:String) => {
        val arrs = row.split("\\t", -1)
        arrs(0) +: arrs(1) +: arrs(2).split(dataSplitter,-1)
      })
    val validRow = rows
      .filter( arr => arr.length == header.length)
      .map(fields => Row(fields: _*))

    dropHDFiles(s"$invalidRowPath$logPath")
    val invalidRows = rows.filter( row => row.length != header.length)
      .map(row => s"invalid row size: ${row.length}, content: ${row.mkString(",")}")
    invalidRows.saveAsTextFile(s"$invalidRowPath$logPath")

    val struct = StructType(header)
    spark.createDataFrame(validRow, struct)

  }

  def readCsv(path:String, delimiter:String): Dataset[Row] = {
    spark.read
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(path)

  }

  def estimator[T](rdd: RDD[T]): Long = {
    SizeEstimator.estimate(rdd)
  }



}
