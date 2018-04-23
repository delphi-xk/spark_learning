package com.hyzs.spark.ml


import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.{BaseUtil, JsonUtil, Params, SparkUtils}
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
    
    val df = spark.sql("select * from test_data")

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
    val df = spark.sql("select * from test_data")
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

  def getIndexers(df: Dataset[Row], col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col,indexer)
  }


  def castStringType(df:Dataset[Row], col:String): (Dataset[Row], StringIndexerModel) = {
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

  def castDFdtype(df:Dataset[Row], colName:String, dType:DataType): Dataset[Row] = {
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

  def replaceIndexedCols(df:Dataset[Row], cols:Seq[String]): Dataset[Row] = {
    val remainCols = df.columns diff cols
    val replaceExprs = cols.map( col => s" ${col}_indexer as $col")
    df.selectExpr(remainCols ++: replaceExprs: _*)
  }

  def dropOldCols(df:Dataset[Row],
                  stringCols:Seq[String],
                  timeCols:Seq[String],
                  numberCols:Seq[String]): Option[Dataset[Row]] = {
    // add string index start with 1
    val strExprs = stringCols.map(col => s" (${col}_indexer + 1) as $col")
    val timeExprs = timeCols.map(col => s" ${col}_stamp as $col")
    val numberExprs = numberCols.map(col =>  s" ${col}_number as $col")
    Some(df.selectExpr(strExprs ++: timeExprs ++: numberExprs :_*))
  }

  def castTimestampFuc(time:String): Long = {
    BaseUtil.getUnixStamp(time).getOrElse(0)
  }

  def saveLibsvmFile_old(df:Dataset[Row]): Unit = {
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
    val table = spark.createDataFrame(data, struct)
    saveTable(table, "jd_test_data")

    val label = createDFfromRawCsv(Array(key,"label"), "/hyzs/test/test.index")
    saveTable(label, "jd_test_label")
  }

  def buildObjRdd(dataSchema:StructType,
                  indexerArray:Seq[(String,StringIndexerModel)]): RDD[String] = {
    val objList:ListBuffer[BaseObj] = new ListBuffer
    objList += Ob1(0, Params.NO_TYPE, key)
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
        case IntegerType => Ob1(index, Params.NUMERIC_TYPE, field.name)
        case DoubleType => Ob1(index, Params.NUMERIC_TYPE, field.name)
        case DateType => Ob1(index, Params.DATE_TYPE, field.name)
        case TimestampType => Ob1(index, Params.DATE_TYPE, field.name)
        case StringType => Ob2(index, Params.STRING_TYPE, field.name,
          indexerMap.getOrElse(field.name, Map("null"->0)))
        case _ => Ob1(index, Params.NUMERIC_TYPE, field.name)
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

  def readObj(filePath:String): Array[Ob1] = {
    val objRdd = sc.textFile(filePath)
    val objList = objRdd.mapPartitions({ records =>
      val mapper = new ObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      records.map(record => mapper.readValue(record, classOf[Ob1]))
    }).collect()
    objList
  }

  def getStructToJson(dataSchema:StructType): RDD[String] = {
    val structArray = (1 to dataSchema.length).zip(dataSchema).map{ case (index,field) =>
      val info = StructInfo(index, field.name, field.dataType.typeName)
      broadMapper.value.registerModule(DefaultScalaModule)
      broadMapper.value.writeValueAsString(info)
    }
    sc.makeRDD(structArray)
  }

  def castLibsvmString(label:String="0.0", row: Row): String = {
    val datum = row.toSeq
    val resString = new StringBuilder(label)
    datum.zipWithIndex.foreach{ case (field,i) =>
      if(field!= null && field != 0.0){
        val digit = new BigDecimal(field.toString)
        resString += ' '
        resString ++= s"${i+1}:${digit.toPlainString}"
      }
    }
    resString.toString()
  }

  def main(args: Array[String]): Unit = {
    val tables = conf.get("spark.processJob.tableNames").split(",")
    val convertPath = "/hyzs/model/"
    for(tableName <- tables){
      val taskPath = s"$convertPath$tableName/"
      val objPath = s"${taskPath}obj"
      val modelPath = s"${taskPath}model"
      val libsvmPath = s"${taskPath}libsvm"
      val indexPath = s"${taskPath}index"
      val namePath = s"${taskPath}name"
      val resultPath = s"${taskPath}result"

      val sourceData = spark.table(tableName).repartition(200, col(key))
      val allLabel = spark.table("m1_label").repartition(200, col(key))
      // filter label based on source data
      val fullData = allLabel.join(sourceData, Seq(key), "left").repartition(200, col(key))

      val labelRdd:RDD[String] = fullData.select("label").rdd.map(row => row(0).toString)
      val indexRdd:RDD[String] = fullData.select(key).rdd.map(row => row(0).toString)
      val data = fullData.drop(key).drop("label")
        .na.fill("0.0")
        .na.replace("*", Map("" -> "0.0", "null" -> "0.0"))
      val nameRdd = sc.makeRDD[String](sourceData.columns)

      val dataColsArray = data.columns
      var stringCols = Seq[String]()
      var timeCols = Seq[String]()
      var numberCols = Seq[String]()
      var result:Dataset[Row] = null

      if(args.length >0 && args(0) == "import_table"){
        result = spark.table(s"${tableName}_libsvm")
      }
      else{
        if(args.length >1 && args(1) == "predict"){
          //val pipeline = Pipeline.load(modelPath)
          //result = Some(pipeline.fit(data).transform(data))
          //val objList = readObj(s"$resultPath/$tableName.obj")

          val dataSchema: StructType = data.schema
          val stringSchema = dataSchema.filter(field => field.dataType == StringType)
          val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)
          stringCols = stringSchema.map(field => field.name)
          timeCols = timeSchema.map(field => field.name)
          numberCols = data.columns diff stringCols diff timeCols

          val indexerArray = stringCols.map(field => getIndexers(data, field))
          val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))
          result = pipeline.fit(data).transform(data)

        } else if(args.length >1 && args(1) == "train"){
          // spark 1.6 inferSchema removed
          val dataSchema: StructType = data.schema
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

          result = pipeline.fit(data).transform(data)

          saveRdd(objRdd, objPath)
          saveRdd(nameRdd, namePath)
          mkHDdir(resultPath)
          copyMergeHDFiles(s"$objPath/", s"$resultPath/$tableName.obj")
          copyMergeHDFiles(s"$namePath/", s"$resultPath/$tableName.name")
        }

        val stampUdf = udf(castTimestampFuc _)
        for(col <- timeCols){
          result = result.withColumn(s"${col}_stamp", stampUdf(result(col)))
        }

        for(col <- numberCols){
          result = result.withColumn(s"${col}_number", result(col).cast(DoubleType))
        }
        result = dropOldCols(result, stringCols, timeCols, numberCols).get
        result = result.selectExpr(dataColsArray:_*)
        saveTable(result, s"${tableName}_libsvm")
      }

      // zip rdd should have THE SAME partitions
      /*    val libsvm = result.get.rdd.zip(labelRdd).map{
            case (row, i) => castLibsvmString(i, row)
          }*/

      val libsvm: RDD[String] = result.rdd.zip(labelRdd).mapPartitions(rows => {
        rows.map{ case(row, i) => castLibsvmString(i, row)}
      })

      saveRdd(libsvm, libsvmPath)
      saveRdd(indexRdd, indexPath)
      mkHDdir(resultPath)
      copyMergeHDFiles(s"$libsvmPath/", s"$resultPath/$tableName.libsvm")
      copyMergeHDFiles(s"$indexPath/", s"$resultPath/$tableName.index")
    }

  }
}


