package com.hyzs.spark.ml

import java.math.BigDecimal

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hyzs.spark.bean._
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.{BaseUtil, Params}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.hyzs.spark.sql.JDDataProcess
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by XIANGKUN on 2018/4/24.
  */

object ConvertLibsvm_v2 {

  val originalKey = "user_id"
  //val key = "user_id_md5"
  val key = "id"
  val maxLabelMapLength = 20
  import spark.implicits._

  def getIndexers(df: Dataset[Row], col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col,indexer)
  }

  def strToIndex(labels:Array[String]):UserDefinedFunction = udf((str:String) => {
    labels.zipWithIndex
      .toMap
      .getOrElse(str, 0)
  })


  def replaceIndexedCols(df:Dataset[Row], cols:Seq[String]): Dataset[Row] = {
    val remainCols = df.columns diff cols
    val replaceExprs = cols.map( col => s" ${col}_indexer as $col")
    df.selectExpr(remainCols ++: replaceExprs: _*)
  }

  def replaceOldCols(df:Dataset[Row],
                  idCols:Seq[String],
                     labelArray:Seq[(String, Array[String])],
                  timeCols:Seq[String],
                  numberCols:Seq[String],
                  allCols:Seq[String]): Dataset[Row] = {

    // spark.udf.register("stamp", castTimestampFuc _)
    //val timeUdf = udf(castTimestampFuc _)

    val ids = idCols.map(col)
    val strings = labelArray.map{ datum =>
      strToIndex(datum._2)(col(datum._1)).as(datum._1)
    }
    val times = timeCols.map(colName => unix_timestamp(col(colName), "yyyy-MM-dd' 'HH:mm:ss").as(colName))
    val numbers = numberCols.map(colName => col(colName).cast("double").as(colName))

    df.select(ids ++: strings ++:times ++: numbers :_*)
      .selectExpr(allCols: _*)
  }

  def castTimestampFuc(time:String): Long = {
    BaseUtil.getUnixStamp(time).getOrElse(0)
  }

  def saveRdd(rdd:RDD[String], savePath:String): Unit = {
    if(checkHDFileExist(savePath))
      dropHDFiles(savePath)
    rdd.saveAsTextFile(savePath)
  }

  def buildObjRdd(dataSchema:Seq[StructField],
                  indexerArray:Seq[(String,StringIndexerModel)]): RDD[String] = {
    val objList:ListBuffer[BaseObj] = new ListBuffer
    objList += Ob1(0, Params.NO_TYPE, key)
    val indexerMap: Map[String,Map[String,Int]] = indexerArray.map{ case (name,model) =>
      if(model.labels.length<maxLabelMapLength)
        (name, model.labels.zip(1 to model.labels.length).toMap)
      else{
        val labels = model.labels.slice(0, maxLabelMapLength)
        (name, labels.zip(1 to maxLabelMapLength).toMap)
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
      //.sortBy(obj => obj.key)
    val objStr = objRdd.mapPartitions( objs => {
      val mapper = new ObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      objs.map(obj => mapper.writeValueAsString(obj))
    })
    objStr

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

  def castLibsvmString(label:String="0.0", datum: Seq[Any]): String = {
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

  def prepareDataTable(): Unit ={
    val data = spark.table("sample_all")
    JDDataProcess.trainModelData(processNull(data))
  }

  def convertLibsvm(): Unit ={
    val args = Array("no_import", "train")
    val tables = Seq("m1_test")
    val labels = Seq("m1_label")

    val convertPath = "/hyzs/model/"
    for((tableName, labelName) <- tables.zip(labels)){
      val taskPath = s"$convertPath$tableName/"
      val objPath = s"${taskPath}obj"
      val modelPath = s"${taskPath}model"
      val libsvmPath = s"${taskPath}libsvm"
      val indexPath = s"${taskPath}index"
      val namePath = s"${taskPath}name"
      val resultPath = s"${taskPath}result"

      val sourceData = spark.table(tableName)
      val allLabel = spark.table(labelName).randomSplit(Array(0.8,0.2))
      val label = allLabel(0)
      val labelForTest = allLabel(1)
      saveTable(labelForTest, "m1_label_test")
      // join process based on id in data table
      val fullData = label.join(sourceData, Seq(key), "left")

      val labelRdd:RDD[String] = fullData.select("label").rdd.map(row => row(0).toString)
      val indexRdd:RDD[String] = fullData.select(key).rdd.map(row => row(0).toString)
      var result:Dataset[Row] = fullData //.drop(key).drop("label")
        .na.fill(0.0)
        .na.fill("0.0")
        .na.replace("*", Map("" -> "0.0", "null" -> "0.0"))
      val nameRdd = sc.makeRDD[String](sourceData.columns)

      val allCols = result.columns
      val idAndLabelCols = result.columns.take(2)
      val dataCols = result.columns.drop(2)
      val dataSchema: Seq[StructField] = result.schema.drop(2)
      val stringSchema = dataSchema.filter(field => field.dataType == StringType)
      val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)
      val stringCols = stringSchema.map(field => field.name)
      val timeCols = timeSchema.map(field => field.name)
      val numberCols = dataCols diff stringCols diff timeCols
      val indexerArray = stringCols.map(field => getIndexers(result, field))
      val labelArray:Seq[(String, Array[String])] = indexerArray.map(datum => (datum._1, datum._2.labels))

      //val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))

      if(args.length >1 && args(1) == "predict"){
        val pipeline = Pipeline.load(modelPath)
        result = pipeline.fit(result).transform(result)

      } else if(args.length >1 && args(1) == "train"){

        val objRdd = buildObjRdd(dataSchema, indexerArray)

        if(checkHDFileExist(modelPath))dropHDFiles(modelPath)

        //pipeline.save(modelPath)
        //result = pipeline.fit(result).transform(result)

        println("start save obj...")
        saveRdd(objRdd, objPath)
        println("start save name...")
        saveRdd(nameRdd, namePath)
        mkHDdir(resultPath)
        copyMergeHDFiles(s"$objPath/", s"$resultPath/$tableName.obj")
        copyMergeHDFiles(s"$namePath/", s"$resultPath/$tableName.name")
        println("save obj name finished.")
      }

      result = replaceOldCols(result, idAndLabelCols, labelArray, timeCols, numberCols, allCols)
      saveTable(result, "m1_test_libsvm")

     /* println("start save libsvm file")
      val libsvm: RDD[String] = result.rdd.map(row => {
          val datum = row.toSeq
          castLibsvmString(datum(1).toString, datum.drop(2))
      })

      saveRdd(libsvm, libsvmPath)
      saveRdd(indexRdd, indexPath)
      println("save libsvm file finished.")
      mkHDdir(resultPath)
      copyMergeHDFiles(s"$libsvmPath/", s"$resultPath/$tableName.libsvm")
      copyMergeHDFiles(s"$indexPath/", s"$resultPath/$tableName.index")
      println("merge file finished.")*/
    }
  }

  def main(args: Array[String]): Unit = {
    //prepareDataTable
    convertLibsvm()

  }
}


