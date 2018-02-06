package com.hyzs.spark.ml




import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.{BaseUtil, InferSchema, SparkUtils}
import java.math.BigDecimal

/**
  * Created by XIANGKUN on 2017/12/5.
  */
object MLtest {


  val originalKey = "user_id"
  val key = "user_id_md5"

  def convertDFtoLibsvm(): Unit = {
/*    val df = sqlContext.sql("select * from test_data")
    val labelArray = df.select("jdmall_user_p0003").distinct.map(_.getString(0)).collect
    val labelMap = labelArray.zipWithIndex.toMap*/

/*    val label = sqlContext.sql("select * from consume_label limit 100")
    val test = label.join(df, Seq("user_id"), "left_outer")
    val rdd_test: RDD[Vector] = test.rdd.map( row => Vectors.dense(row.getDouble(0), row.getDouble(2)) )*/

/*   val indexer = new StringIndexer()
      .setInputCol("jdmall_user_p0003")
      .setOutputCol("p0003_indexer")
      .setHandleInvalid("skip")
      .fit(df)
      val converter = new IndexToString()
         .setInputCol("p0003_indexer")
         .setOutputCol("p0003_converter")
         .setLabels(indexer.labels)
       val indexer2 = new StringIndexer()
         .setInputCol("mkt_schd_p0001328")
         .setOutputCol("p0001328_indexer")
         .setHandleInvalid("skip")
         .fit(df)
       val converter2 = new IndexToString()
         .setInputCol("p0001328_indexer")
         .setOutputCol("p0001328_converter")
         .setLabels(indexer2.labels)

       val pipeline = new Pipeline()
         .setStages(Array(indexer, indexer2, converter, converter2))
         .fit(df)
       val res = pipeline.transform(df).select("user_id",
         "jdmall_user_p0003","p0003_indexer", "p0003_converter",
         "mkt_schd_p0001328","p0001328_indexer","p0001328_converter")*/

    val df = sqlContext.sql("select * from test_data")

    val key = "user_id"
    //val colCounts = df.columns.map(df.select(_).distinct().count())
    val cols = df.columns.drop(1)
    val indexerArray = cols
      .map(col => getIndexers(df, col))
      .filter(_._2.labels.length < 100)
    val indexedCols = indexerArray.map(_._1)
    val noIndexedCols = cols diff indexedCols

    val pipeline = new Pipeline()
      .setStages(Array(indexerArray.map(_._2): _*))
      .fit(df)
    val indexedTable = pipeline
      .transform(df)
      .select(key, (noIndexedCols ++: indexedCols.map(col => s"${col}_indexer")): _*)
    //  .write.option("path","/hyzs/warehouse/test_result").saveAsTable("test_result")

  }

  def convertToLibsvm(): Unit ={
    val df = sqlContext.sql("select * from test_data")
    val cols = df.columns.drop(1)
    val indexerArray = cols
      .map(col => getIndexers(df, col))
    val pipeline = new Pipeline()
      .setStages(Array(indexerArray.map(_._2): _*))
    val model1 = pipeline.fit(df)
    val transformed = model1.transform(df)

    //transformed.columns.filter(str => str.matches("\\S+_indexer"))
    val indexerCols = transformed.columns.filter(str => str.endsWith("_indexer"))

    val assembler = new VectorAssembler()
      .setInputCols(indexerCols)
      .setOutputCol("features")

    val pipeline2 = new Pipeline().setStages(Array(assembler))
    val model2 = pipeline2.fit(transformed)
    val res = model2.transform(transformed).selectExpr("client_no" +: indexerCols :+ "features": _*)
    val labeledFunc: (Vector => LabeledPoint) = (vector: Vector) =>{
      LabeledPoint(0.0, vector)
    }

    val labelData = res.select("features").rdd.map{ x: Row => x.getAs[Vector](0)}.map(labeledFunc)

    MLUtils.saveAsLibSVMFile(labelData.coalesce(1), "/hyzs/data/test_libsvm")

  }

  def getIndexers(df: DataFrame, col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col,indexer)
  }

  // TODO: row type cast based on castArray
  def castRowType(row: Row, castArray: Seq[DataType]): Row = {
    assert(row.length == castArray.length)
    val rowData = row.toSeq.zip(castArray)
    val newRow = rowData.map{ case (datum, dType) =>
      val newData = (datum, dType) match {
        case ("", _) => "0"
        case (_, TimestampType) => ""
      }
      newData
    }
    Row.fromSeq(newRow)
  }

  def castStringType(df:DataFrame, col:String): (DataFrame, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    val transformed = indexer.transform(df)
    val res = transformed.withColumn(col, transformed(s"${col}_indexer")).drop(s"${col}_indexer")
    (res, indexer)
  }

  val indexerArray = new ArrayBuffer[StringIndexerModel]

  def castDFdtype(df:DataFrame, colName:String, dType:DataType): DataFrame = {
    assert(df.columns contains colName)
    val df_new = dType match {
      case StringType =>
        val (res, indexer) = castStringType(df, colName)
        indexerArray.append(indexer)
        res
      case TimestampType => df.withColumn(s"$colName", unix_timestamp(df(s"$colName")))
      case _ => df.withColumn(s"$colName", df(s"$colName").cast(DoubleType))
    }
    df_new
  }

  def replaceIndexedCols(df:DataFrame, cols:Seq[String]): DataFrame = {
    val remainCols = df.columns diff cols
    val replaceExprs = cols.map( col => s" ${col}_indexer as $col")
    df.selectExpr(remainCols ++: replaceExprs: _*)
  }

  def dropOldCols(df:DataFrame, stringCols:Seq[String], timeCols:Seq[String], numberCols:Seq[String]): DataFrame = {
    // add string index start with 1
    val strExprs = stringCols.map(col => s" (${col}_indexer + 1) as $col")
    val timeExprs = timeCols.map(col => s" ${col}_stamp as $col")
    val numberExprs = numberCols.map(col =>  s" ${col}_number as $col")
    df.selectExpr(strExprs ++: timeExprs ++: numberExprs :_*)
  }

  def castTimestampFuc(time:String): Long = {
    BaseUtil.getUnixStamp(time).getOrElse(0)
  }

  def castLibsvmString(label:String="0.0", row: Row): String = {
    val datum = row.toSeq
    val resString = new StringBuilder(label)
    datum.zipWithIndex.foreach{ case (field,i) => {
        if(field != 0.0){
          val digit = new BigDecimal(field.toString)
          resString += ' '
          resString ++= s"${i+1}:${digit.toPlainString}"
        }
      }
    }
    resString.toString()
  }

  def saveLibsvmFile_old(df:DataFrame): Unit = {
    val assembler = new VectorAssembler()
      .setInputCols(df.columns)
      .setOutputCol("features")

    val pipeline2 = new Pipeline().setStages(Array(assembler))
    val model2 = pipeline2.fit(df)
    val res = model2.transform(df).select("features")

    val labeledFunc: (Vector => LabeledPoint) = (vector: Vector) =>{
      LabeledPoint(0.0, vector)
    }

    val labelData = res.select("features").rdd.map{ x: Row => x.getAs[Vector](0)}.map(labeledFunc)

    MLUtils.saveAsLibSVMFile(labelData.coalesce(1), "/hyzs/data/test_libsvm")
  }

  def saveRdd(rdd:RDD[String], savePath:String): Unit = {
    if(checkHDFileExist(savePath))
      dropHDFiles(savePath)
    rdd.coalesce(1)
      .saveAsTextFile(savePath)
  }

  def import_data(): Unit = {
    val header = sc.textFile("/hyzs/test/fea.header").first().split(",").map(col => StructField(col, StringType))
    val dataFile = sc.textFile("/hyzs/test/feature_data.txt")
    val data = dataFile
      .filter(row => !row.isEmpty)
      .map( (row:String) => {
        val arr = row.split("\\t", -1)
        arr(0) +: arr(1) +: arr(2).split(",", -1)
      })
      .filter( arr => arr.length <= header.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(header)
    val table = sqlContext.createDataFrame(data, struct)
    saveTable(table, "jd_test_data")
  }

  def main(args: Array[String]): Unit = {
    if(args.length >0 && args(0) == "import"){
      import_data()
    }
    val df = sqlContext.table("hyzs.jd_test_data").drop(originalKey)

    val index = df.select(key).rdd.map(row => row.getString(0))
    val name = sc.makeRDD[String](df.columns)
    val data = df.drop(key).na.fill("0")
      .na.replace("*", Map("" -> "0", "null" -> "0"))
    val dataSchema = InferSchema.inferSchema(data)
    val stringSchema = dataSchema.filter(field => field.dataType == StringType)
    val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)
    val stampUdf = udf(castTimestampFuc _)

    val indexerArray = stringSchema.map(field => getIndexers(data, field.name))
    val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))
    val stringCols = stringSchema.map(field => field.name)
    val timeCols = timeSchema.map(field => field.name)
    val numberCols = data.columns diff stringCols diff timeCols
    val stringModels = pipeline.fit(data)
    var result = stringModels.transform(data)

    for(col <- timeCols){
      result = result.withColumn(s"${col}_stamp", stampUdf(result(col)))
    }

    for(col <- numberCols){
      result = result.withColumn(s"${col}_number", result(col).cast(DoubleType))
    }

    result = dropOldCols(result, stringCols, timeCols, numberCols)
    saveTable(result, "jd_test_result")

    // zip rdd should have THE SAME partitions
    val libsvmff = result.rdd.zip(index).map{
      case (row, i) => castLibsvmString(i, row)
    }

    saveRdd(libsvmff, "/hyzs/data/test_libsvm")
    saveRdd(index, "/hyzs/data/test_index")
    saveRdd(name, "/hyzs/data/test_name")
  }
}
