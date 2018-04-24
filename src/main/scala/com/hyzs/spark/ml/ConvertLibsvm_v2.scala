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


/**
  * Created by XIANGKUN on 2018/4/24.
  */

object ConvertLibsvm_v2 {

  val originalKey = "user_id"
  val key = "user_id_md5"
  val maxLabelMapLength = 20

  def getIndexers(df: Dataset[Row], col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col,indexer)
  }

  def replaceIndexedCols(df:Dataset[Row], cols:Seq[String]): Dataset[Row] = {
    val remainCols = df.columns diff cols
    val replaceExprs = cols.map( col => s" ${col}_indexer as $col")
    df.selectExpr(remainCols ++: replaceExprs: _*)
  }

  def dropOrgCols(df:Dataset[Row],
                  idCols:Seq[String],
                  stringCols:Seq[String],
                  timeCols:Seq[String],
                  numberCols:Seq[String]): Option[Dataset[Row]] = {
    // add string index start with 1
    val strExprs = stringCols.map(col => s" (${col}_indexer + 1) as $col")
    val timeExprs = timeCols.map(col => s" ${col}_stamp as $col")
    val numberExprs = numberCols.map(col =>  s" ${col}_number as $col")
    Some(df.selectExpr(idCols ++: strExprs ++: timeExprs ++: numberExprs :_*))
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

  def main(args: Array[String]): Unit = {
    //val tables = Seq("m1_test", "m2_test", "m3_test")
    //val labels = Seq("m1_label", "m2_label", "m3_label")
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
      val label = spark.table(labelName)
      val fullData = label.join(sourceData, Seq(key), "left")

      val labelRdd:RDD[String] = fullData.select("label").rdd.map(row => row(0).toString)
      val indexRdd:RDD[String] = fullData.select(key).rdd.map(row => row(0).toString)
      var result:Dataset[Row] = fullData //.drop(key).drop("label")
        .na.fill("0.0")
        .na.replace("*", Map("" -> "0.0", "null" -> "0.0"))
      val nameRdd = sc.makeRDD[String](sourceData.columns)

      val allCols = result.columns
      val idCols = result.columns.take(2)
      val dataCols = result.columns.drop(2)
      val dataSchema: Seq[StructField] = result.schema.drop(2)
      val stringSchema = dataSchema.filter(field => field.dataType == StringType)
      val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)
      val stringCols = stringSchema.map(field => field.name)
      val timeCols = timeSchema.map(field => field.name)
      val numberCols = dataCols diff stringCols diff timeCols
      val indexerArray = stringSchema.map(field => getIndexers(result, field.name))
      val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))

      if(args.length >0 && args(0) == "import_table"){
        result = spark.table(s"${tableName}_libsvm")
      }
      else{
        if(args.length >1 && args(1) == "predict"){
          val pipeline = Pipeline.load(modelPath)
          result = pipeline.fit(result).transform(result)

        } else if(args.length >1 && args(1) == "train"){
          // spark 2.2 inferSchema removed
          val objRdd = buildObjRdd(dataSchema, indexerArray)

          if(checkHDFileExist(modelPath))dropHDFiles(modelPath)
          pipeline.save(modelPath)

          result = pipeline.fit(result).transform(result)
          println("start save obj...")
          saveRdd(objRdd, objPath)
          println("start save name...")
          saveRdd(nameRdd, namePath)
          mkHDdir(resultPath)
          copyMergeHDFiles(s"$objPath/", s"$resultPath/$tableName.obj")
          copyMergeHDFiles(s"$namePath/", s"$resultPath/$tableName.name")
          println("save obj name finished.")
        }

        val stampUdf = udf(castTimestampFuc _)
        for(col <- timeCols){
          result = result.withColumn(s"${col}_stamp", stampUdf(result(col)))
        }

        for(col <- numberCols){
          result = result.withColumn(s"${col}_number", result(col).cast(DoubleType))
        }
        result = dropOrgCols(result, idCols, stringCols, timeCols, numberCols).get
        // set original column order
        result = result.selectExpr(allCols:_*)
        println("start save libsvm table")
        saveTable(result, s"${tableName}_libsvm")
      }

      println("start save libsvm file")
      val libsvm: RDD[String] = result.rdd.mapPartitions(rows => {
        rows.map{ row => {
          val datum = row.toSeq
          castLibsvmString(datum(1).toString, datum.drop(2))
        }}
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
}


