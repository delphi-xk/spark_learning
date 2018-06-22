package com.hyzs.spark.ml


import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.{BaseUtil, InferSchema, JsonUtil, Params, SparkUtils}
import java.math.BigDecimal

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hyzs.spark.bean._
import org.apache.spark.util.SizeEstimator


/**
  * Created by XIANGKUN on 2017/12/5.
  */



object ConvertLibsvm {


  val originalKey = "user_id"
  val key = "user_id_md5"
  //val fileName = "part-00000"
  val maxLabelLength = 10
  def convertDFtoLibsvm(): Unit = {

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

  def dropOldCols(df:DataFrame,
                  stringCols:Seq[String],
                  timeCols:Seq[String],
                  numberCols:Seq[String]): Option[DataFrame] = {
    // add string index start with 1
    val strExprs = stringCols.map(col => s" (${col}_indexer + 1) as $col")
    val timeExprs = timeCols.map(col => s" ${col}_stamp as $col")
    val numberExprs = numberCols.map(col =>  s" ${col}_number as $col")
    Some(df.selectExpr(strExprs ++: timeExprs ++: numberExprs :_*))
  }

  def castTimestampFuc(time:String): Long = {
    BaseUtil.getUnixStamp(time).getOrElse(0)
  }

  def castLibsvmString(label:String="0.0", row: Row): String = {
    val datum = row.toSeq
    val resString = new StringBuilder(label)
    datum.zipWithIndex.foreach{ case (field,i) =>
        if(field != 0.0){
          val digit = new BigDecimal(field.toString)
          resString += ' '
          resString ++= s"${i+1}:${digit.toPlainString}"
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
    rdd.saveAsTextFile(savePath)
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

    val label = createDFfromRawCsv(Array(key,"label"), "/hyzs/test/test.index")
    saveTable(label, "jd_test_label")
  }

  def buildObjRdd(dataSchema:StructType,
                  indexerArray:Seq[(String,StringIndexerModel)]): RDD[String] = {
    val objList:ListBuffer[ModelObject] = new ListBuffer
    objList += ModelObject(0, Params.NO_TYPE, key, Map())
    val indexerMap: Map[String,Map[String,Int]] = indexerArray.map{ case (name,model) =>
      if(model.labels.length<maxLabelLength)
        (name, model.labels.zip(1 to model.labels.length).toMap)
      else{
        val labels = model.labels.slice(0, maxLabelLength)
        (name, labels.zip(1 to maxLabelLength).toMap)
      }
    }.toMap
    (1 to dataSchema.length).zip(dataSchema).foreach{ case (index, field) =>
      val obj = field.dataType match {
        case IntegerType => ModelObject(index, Params.NUMERIC_TYPE, field.name, Map())
        case DoubleType => ModelObject(index, Params.NUMERIC_TYPE, field.name, Map())
        case DateType => ModelObject(index, Params.DATE_TYPE, field.name, Map())
        case TimestampType => ModelObject(index, Params.DATE_TYPE, field.name, Map())
        case StringType => ModelObject(index, Params.STRING_TYPE, field.name,
          indexerMap.getOrElse(field.name, Map("null"->0)))
        case _ => ModelObject(index, Params.NUMERIC_TYPE, field.name, Map())
      }
      objList += obj
    }
    val objRdd = sc.parallelize(objList)
        //.coalesce(1, true)
      .sortBy(obj => obj.key)
    val objStr = objRdd.mapPartitions( objs => {
      val mapper = new ObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      objs.map(obj => mapper.writeValueAsString(obj))
    })
    objStr

/*    val resString = objList.map( obj => {
      broadMapper.value.registerModule(DefaultScalaModule)
      broadMapper.value.writeValueAsString(obj)
    })
    sc.makeRDD[String](resString)*/

  }

  def readObj(filePath:String): Array[ModelObject] = {
    val objRdd = sc.textFile(filePath)
    val objList = objRdd.mapPartitions({ records =>
      val mapper = new ObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      records.map(record => mapper.readValue(record, classOf[ModelObject]))
    }).collect()
    objList
  }


  def main(args: Array[String]): Unit = {
    val tableName = "hyzs.jd_test_data"
    val convertPath = "/hyzs/convert_data/"
    val taskPath = s"$convertPath$tableName/"
    val objPath = s"${taskPath}obj"
    val modelPath = s"${taskPath}model"
    val libsvmPath = s"${taskPath}libsvm"
    val indexPath = s"${taskPath}index"
    val namePath = s"${taskPath}name"
    val resultPath = s"${taskPath}result"

    if(args.length >1 && args(1) == "import"){
      import_data()
    }
    val sourceData = sqlContext.table(tableName).drop(originalKey)
    val allLabel = sqlContext.table("hyzs.jd_test_label")
    // filter label based on source data
    val fullData = allLabel.join(sourceData, Seq(key), "right")

    val labelRdd:RDD[String] = fullData.select("label").rdd.map(row => row(0).toString)
    val indexRdd:RDD[String] = fullData.select(key).rdd.map(row => row(0).toString)
    val data = fullData.drop(key).drop("label").na.fill("0")
      .na.replace("*", Map("" -> "0", "null" -> "0"))
    val nameRdd = sc.makeRDD[String](sourceData.columns)

    val dataColsArray = data.columns
    var stringCols = Seq[String]()
    var timeCols = Seq[String]()
    var numberCols = Seq[String]()
    var result: Option[DataFrame] = None
    if(args.length >0 && args(0) == "predict"){
      val pipeline = Pipeline.load(modelPath)
      result = Some(pipeline.fit(data).transform(data))

      val objList = readObj(s"$resultPath/$tableName.obj")
      stringCols = objList.filter(obj => obj.fieldType == Params.STRING_TYPE).map(_.fieldName)
      timeCols = objList.filter(obj => obj.fieldType == Params.DATE_TYPE).map(_.fieldName)
      numberCols = objList.filter(obj => obj.fieldType == Params.NUMERIC_TYPE).map(_.fieldName)

    } else if(args.length >0 && args(0) == "train"){
      val dataSchema = InferSchema.inferSchema(data)
      val stringSchema = dataSchema.filter(field => field.dataType == StringType)
      val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)

      val indexerArray = stringSchema.map(field => getIndexers(data, field.name))
      val objRdd = buildObjRdd(dataSchema, indexerArray)
      val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))
      stringCols = stringSchema.map(field => field.name)
      timeCols = timeSchema.map(field => field.name)
      numberCols = data.columns diff stringCols diff timeCols

      if(checkHDFileExist(modelPath))dropHDFiles(modelPath)
      pipeline.save(modelPath)

      result = Some(pipeline.fit(data).transform(data))

      saveRdd(objRdd, objPath)
      saveRdd(nameRdd, namePath)
      mkHDdir(resultPath)
      copyMergeHDFiles(s"$objPath/", s"$resultPath/$tableName.obj")
      copyMergeHDFiles(s"$namePath/", s"$resultPath/$tableName.name")
    }

    val stampUdf = udf(castTimestampFuc _)
    for(col <- timeCols){
      result = Some(result.get.withColumn(s"${col}_stamp", stampUdf(result.get(col))))
    }

    for(col <- numberCols){
      result = Some(result.get.withColumn(s"${col}_number", result.get(col).cast(DoubleType)))
    }
    result = dropOldCols(result.get, stringCols, timeCols, numberCols)
    result = Some(result.get.selectExpr(dataColsArray:_*))
    saveTable(result.get, "jd_test_result")

    // zip rdd should have THE SAME partitions
    val libsvm = result.get.rdd.zip(labelRdd).map{
      case (row, i) => castLibsvmString(i, row)
    }


    saveRdd(libsvm, libsvmPath)
    saveRdd(indexRdd, indexPath)
    mkHDdir(resultPath)
    copyMergeHDFiles(s"$libsvmPath/", s"$resultPath/$tableName.libsvm")
    copyMergeHDFiles(s"$indexPath/", s"$resultPath/$tableName.index")

  }
}


