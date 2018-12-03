package com.hyzs.spark.ml

import java.math.BigDecimal

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hyzs.spark.bean._
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.{BaseUtil, Params}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.hyzs.spark.sql.JDDataProcess


/**
  * Created by XIANGKUN on 2018/4/24.
  */

object ConvertLibsvm {

  val originalKey = "user_id"
  //val key = "user_id_md5"
  val key = "card_id"
  val label = "target"
  val maxLabelMapLength = 100
  val convertPath = "/user/hyzs/convert/"

  def getIndexers(df: Dataset[Row], col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col,indexer)
  }

  def strToLabel(stringMap:Map[String,Int])(str:String): Int = {
    stringMap.getOrElse(str, 0)
  }

  // dataSet  id, target, feature1, feature2, ...
  def replaceOldCols(df:Dataset[Row], objArray:Array[ModelObject]): Dataset[Row] = {
    val newColNames:Array[Column] = objArray.map{ obj =>
      val colName:Column = obj.fieldType match {
        case Params.NO_TYPE => col(obj.fieldName)
        case Params.NUMERIC_TYPE => col(obj.fieldName).cast("double").as(obj.fieldName)
        case Params.DATE_TYPE => unix_timestamp(col(obj.fieldName), "yyyy-MM-dd' 'HH:mm:ss")
          .as(obj.fieldName)
        case Params.STRING_TYPE => {
          val stringUdf = udf(strToLabel(obj.fieldMap) _)
          stringUdf(col(obj.fieldName)).as(obj.fieldName)
        }
        case _ => col(obj.fieldName).cast("double").as(obj.fieldName)
      }
      colName
    }
    df.select(newColNames :_*)
  }

  def saveRdd(rdd:RDD[String], savePath:String): Unit = {
    val tmpPath = "/tmp/rdd"
    if(checkHDFileExist(tmpPath))
      dropHDFiles(tmpPath)
    rdd.saveAsTextFile(tmpPath)
    copyMergeHDFiles(tmpPath, savePath)
  }

  // ignoreCols
  def buildObjectArray(ignoreCols: Array[String], dataSchema:Seq[StructField],
                  indexerArray:Seq[(String,StringIndexerModel)]): Array[ModelObject] = {
    val objList:ArrayBuffer[ModelObject] = new ArrayBuffer
    for(ignore <- ignoreCols){
      objList += ModelObject(0, Params.NO_TYPE, ignore, Map())
    }
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
        case _:NumericType => ModelObject(index, Params.NUMERIC_TYPE, field.name, Map())
        case DateType => ModelObject(index, Params.DATE_TYPE, field.name, Map())
        case TimestampType => ModelObject(index, Params.DATE_TYPE, field.name, Map())
        case StringType => ModelObject(index, Params.STRING_TYPE, field.name,
          indexerMap.getOrElse(field.name, Map("null"->0)))
        case _ => ModelObject(index, Params.NUMERIC_TYPE, field.name, Map())
      }
      objList += obj
    }
    objList.toArray
  }

  def buildObjectJsonRdd(objList:Array[ModelObject]): RDD[String] = {
    val objRdd:RDD[ModelObject] = sc.parallelize(objList)
    val objStr:RDD[String] = objRdd.mapPartitions({ iter:Iterator[ModelObject] =>
      val mapper = new ObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      for(obj <- iter) yield mapper.writeValueAsString(obj)
    })
    objStr

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

  def convertLibsvm(args:Array[String]): Unit ={
    //val args = Array("train")
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

      // join process based on id in data table
      val fullData = label.join(sourceData, Seq(key), "left")

      val labelRdd:RDD[String] = fullData.select("label").rdd.map(row => row(0).toString)
      val indexRdd:RDD[String] = fullData.select(key).rdd.map(row => row(0).toString)
      var result:Dataset[Row] = processNull(fullData)
      val nameRdd = sc.makeRDD[String](sourceData.columns)
      var objectArray:Array[ModelObject] = null

      if(args.length >0 && args(0) == "predict"){

        objectArray = readObj(s"$resultPath/$tableName.obj")

      } else if(args.length >0 && args(0) == "train"){
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
        objectArray = buildObjectArray(Array(key), dataSchema, indexerArray)
        val objRdd:RDD[String] = buildObjectJsonRdd(objectArray)

        if(checkHDFileExist(modelPath))dropHDFiles(modelPath)

        println("start save obj...")
        saveRdd(objRdd, objPath)
        println("start save name...")
        saveRdd(nameRdd, namePath)
        mkHDdir(resultPath)
        copyMergeHDFiles(s"$objPath/", s"$resultPath/$tableName.obj")
        copyMergeHDFiles(s"$namePath/", s"$resultPath/$tableName.name")
        println("save obj name finished.")
      }

      result = replaceOldCols(result, objectArray)

      println("start save libsvm file")
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
      println("merge file finished.")
    }
  }

  // dataSet  id, target, feature1, feature2, ...
  def trainObjectArray(dataSet: Dataset[Row], tableName:String):Array[ModelObject] = {
    val taskPath = s"$convertPath$tableName/"

    val dataSchema: Seq[StructField] = dataSet.schema.drop(2)
    val stringSchema = dataSchema.filter(field => field.dataType == StringType)
    val stringCols = stringSchema.map(field => field.name)
    val indexerArray = stringCols.map(field => getIndexers(dataSet, field))
    val objectArray = buildObjectArray(Array(key, label), dataSchema, indexerArray)
    val objRdd:RDD[String] = buildObjectJsonRdd(objectArray)
    val nameRdd = sc.makeRDD[String](dataSet.columns)

    println(s"start save obj: ${objRdd.first()} ...")
    saveRdd(objRdd, s"$taskPath$tableName.obj")
    println(s"start save name: ${nameRdd.first()} ...")
    saveRdd(nameRdd, s"$taskPath$tableName.name")
    objectArray
  }

  // dataSet  id, target, feature1, feature2, ...
  def convertLibsvmFromDataSet(dataSet:Dataset[Row], tableName:String, objs:Array[ModelObject]=null): Unit = {
    val taskPath = s"$convertPath$tableName/"
    //val processedData = processZeroValue(dataSet)
    val sourceData = processNull(dataSet)
    var objectArray:Array[ModelObject] = null
    if(objs == null){
      objectArray = trainObjectArray(sourceData, tableName)
    } else objectArray = objs
    val libsvm_result = replaceOldCols(sourceData, objectArray)
    val indexRdd:RDD[String] = libsvm_result.select(key).rdd.map(row => row(0).toString)
    val libsvmRdd: RDD[String] = libsvm_result.rdd.map(row => {
      val datum = row.toSeq
      castLibsvmString(datum(1).toString, datum.drop(2))
    })
    saveRdd(indexRdd, s"$taskPath$tableName.index")
    saveRdd(libsvmRdd, s"$taskPath$tableName.libsvm")
  }


  def main(args: Array[String]): Unit = {
    val trainName = "train_result"
    val taskPath = s"$convertPath$trainName/"
    val trainData = spark.table(s"merchant.$trainName")
    convertLibsvmFromDataSet(trainData, trainName)

    val testName = "test_result"
    val testData =spark.table(s"merchant.$testName")
    val objs = readObj(s"$taskPath$trainName.obj")
    convertLibsvmFromDataSet(testData, testName, objs)
  }
}


