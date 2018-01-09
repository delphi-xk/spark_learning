package com.hyzs.spark.sql

/**
  * Created by XIANGKUN on 2018/1/9.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

object JDDataProcess {

  val conf = new SparkConf().setAppName("DataProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sqlContext.setConf("spark.sql.shuffle.partitions", "2048")
  val key = "user_id"
  //val log = LogManager.getRootLogger

  def createDFfromRawCsv(header: Array[String], path: String, delimiter: String = ","): DataFrame = {
    val data = sc.textFile(path)
    val cols = header.map( col => StructField(col, StringType))
    if(cols.length == data.first().split(delimiter).length){
      val rows = data.map( lines => lines.split(delimiter))
        .map(fields => Row(fields: _*))
      val struct = StructType(cols)
      sqlContext.createDataFrame(rows, struct)
    } else {
      throw new Exception(s"$path data columns not equal!")
    }
  }

  def createDFfromSeparateFile(headerPath: String, dataPath: String,
                               headerSplitter: String=",", dataSplitter: String="\\t"): DataFrame = {
    val header = sc.textFile(headerPath)
    val fields = header.first().split(headerSplitter)
    createDFfromRawCsv(fields, dataPath, dataSplitter)
  }

  def processHis(hisTable: String): DataFrame = {
    val df1 = sqlContext.sql(s"select * from hyzs.$hisTable")
    val swp = df1
      .groupBy(key).agg(count(key).as("count_id"), avg("user_payable_pay_amount").as("avg_pay_amount"))
      .selectExpr(key, "cast (count_id as string) count_id", "cast (avg_pay_amount as string) avg_pay_amount")
    sqlContext.sql(s"drop table hyzs.${hisTable}_new")
    swp.write.saveAsTable(s"hyzs.${hisTable}_new")
    swp
  }

  // ensure countCols can be counted(cast double)
  def labelGenerateProcess(taskName:String, countCols: Array[String], weight: Array[Double]): Unit = {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.Matrices

    if( countCols.length == weight.length){
      val all_data = sqlContext.sql("select * from hyzs.all_data")
      val selectCols = countCols.map( col => s"cast ($col as double) $col")
      val label_data = all_data.selectExpr(key +: selectCols : _*)
      // vector assembling
      val assembler = new VectorAssembler()
        .setInputCols(countCols)
        .setOutputCol("label_feature")
      val trans = assembler.transform(label_data)
      // scaling vectors
      val scaler = new MinMaxScaler()
        .setInputCol("label_feature")
        .setOutputCol("scaled_feature")
      val scaledModel = scaler.fit(trans)
      val scaledData = scaledModel.transform(trans)
      // calculate label values
      val weightMatrix = Matrices.dense(1, weight.length, weight)
      val multiplyFunc : (Vector => Double) = (scaled: Vector) => {
        weightMatrix.multiply(scaled).toArray.apply(0)
      }
      val multiplyUdf = udf(multiplyFunc)
      val userLabel = scaledData.withColumn("label", multiplyUdf(col("scaled_feature")))
        .select("user_id", "label")
      sqlContext.sql(s"drop table hyzs.${taskName}_label")
      userLabel.write.saveAsTable(s"hyzs.${taskName}_label")
    } else {
      throw new Exception("cols and weight length should be equal!")
    }
  }

  def processNA(df: DataFrame): DataFrame = {
    df.na.fill("\\N")
      .na.replace("*", Map("null" -> "\\N", "NULL" -> "\\N", "-9999" -> "\\N"))
      .dropDuplicates(Seq(key))
  }

  def main(args: Array[String]): Unit = {
    // import txt to DataFrame

    sqlContext.sql("create database hyzs")
    val dataPath = sc.getConf.get("spark.processJob.dataPath")
    val headerPath = sc.getConf.get("spark.processJob.headerPath")
    val tableNames = sc.getConf.get("spark.processJob.fileNames")
    val tables = tableNames.split(",")

    for(tableName <- tables){
      val table = createDFfromSeparateFile(headerPath+tableName+".txt", dataPath+tableNames+".txt")
      sqlContext.sql(s"drop table hyzs.$tableName")
      table.write.saveAsTable(s"hyzs.$tableName")
    }

    // process business table, res start with hisTable
    var res = processHis(tables(0))

    // big table join process
    for(tableName <- tables.drop(1)){
      val table = sqlContext.sql(s"select * from hyzs.$tableName")
      res = res.join(table, Seq(key), "left_outer")
    }
    sqlContext.sql("drop table hyzs.all_data")
    // process NA values, save
    processNA(res).write.saveAsTable("hyzs.all_data")


  }


}
