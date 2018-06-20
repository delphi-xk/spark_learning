package com.hyzs.spark.ml

import java.math.BigDecimal

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hyzs.spark.bean._
import com.hyzs.spark.utils.Params
import com.hyzs.spark.utils.SparkUtilsLocal._
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.{Matrices, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


/**
  * Created by XIANGKUN on 2018/6/20.
  */

object ConvertLibsvmLocal {


  val key = "id"
  val maxLabelMapLength = 100
  val convertPath = "/Users/xiangkun/test_data/"


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
    df.select(newColNames :_*).na.fill(0.0)
  }

  def buildObjectArray(dataSchema:Seq[StructField],
                  indexerArray:Seq[(String,StringIndexerModel)]): Array[ModelObject] = {
    val objList:ArrayBuffer[ModelObject] = new ArrayBuffer
    objList += ModelObject(0, Params.NO_TYPE, key, Map())
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

  def labelGenerateProcess(allData:Dataset[Row],
                           taskName:String,
                           countCols: Array[String],
                           weight: Array[Double]): DataFrame = {
    if( countCols.length == weight.length){
      val selectCols = countCols.map( col => s"cast ($col as double) $col")
      val label_data = processNull(allData)
        .selectExpr(key +: selectCols : _*)

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
        .select(key, "label")
      userLabel
    } else {
      throw new Exception("cols and weight length should be equal!")
    }
  }

  // ensure dataFrame.columns contains filterCols
  // remove label relative columns
  def filterLabelColumns(dataFrame: DataFrame, filterCols:Array[String]): DataFrame = {
    val newCols = dataFrame.columns diff filterCols
    dataFrame.selectExpr(newCols: _*)
  }

  def trainModelData(allData: DataFrame): Unit = {
    val labelProcessMap = Map(
      "m1" ->
        (Array("jdmall_ordr_f0116", "jdmall_user_p0001"), Array(0.5, 0.5)),
      "m2" ->
        (Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008"),
          Array(0.3, 0.3, 0.3, 0.1)),
      "m3" ->
        (Array("mem_vip_f0011", "mem_vip_f0001"), Array(0.5, 0.5))
    )
    for( (task, params) <- labelProcessMap) {
      val labelTable = labelGenerateProcess(allData, task, params._1, params._2)
      saveTable(labelTable, s"${task}_label")
      val dataTable = filterLabelColumns(allData, params._1)
      val splitData = dataTable.randomSplit(Array(0.3, 0.3, 0.4))
      val train = splitData(0)
      val valid = splitData(1)
      val test = dataTable
      saveTable(train, s"${task}_train")
      saveTable(valid, s"${task}_valid")
      saveTable(test, s"${task}_test")
    }
  }


  def prepareDataTable(): Unit ={
    val data = spark.table("sample_all")
    trainModelData(processNull(data))
  }


  def convertLibsvm(args:Array[String]): Unit ={
    //TODO: switch libsvm with or without label table
    //val args = Array("train")
    assert(args.length >= 1, "args length should be no less than 1! ")
    val tables = Seq("m1_test")
    val labels = Seq("m1_label")


    for((tableName, labelName) <- tables.zip(labels)){
      val taskPath = s"$convertPath$tableName/"
      val objPath = s"${taskPath}obj"
      val modelPath = s"${taskPath}model"
      val libsvmPath = s"${taskPath}libsvm"
      val indexPath = s"${taskPath}index"
      val namePath = s"${taskPath}name"
      val resultPath = s"${taskPath}result"

      val sourceData = processNull(spark.table(tableName))
      val label = spark.table(labelName)

      // join process based on id in data table
      //val fullData = label.join(sourceData, Seq(key), "left")
      val indexRdd:RDD[String] = label.select(key).rdd.map(row => row(0).toString)
      val nameRdd = sc.makeRDD[String](sourceData.columns)
      var objectArray:Array[ModelObject] = null

      if(args.length >0 && args(0) == "predict"){

        objectArray = readObj(s"$resultPath/$tableName.obj")

      } else if(args.length >0 && args(0) == "train"){
        val allCols = sourceData.columns
        val idCols = allCols.take(1)
        val dataCols = allCols.drop(1)
        val dataSchema: Seq[StructField] = sourceData.schema.drop(1)
        val stringSchema = dataSchema.filter(field => field.dataType == StringType)
        val timeSchema = dataSchema.filter(field => field.dataType == TimestampType)
        val stringCols = stringSchema.map(field => field.name)
        val timeCols = timeSchema.map(field => field.name)
        val numberCols = dataCols diff stringCols diff timeCols

        val indexerArray = stringCols.map(field => getIndexers(sourceData, field))
        objectArray = buildObjectArray(dataSchema, indexerArray)
        val objRdd:RDD[String] = buildObjectJsonRdd(objectArray)

        println("start save obj...")
        objRdd.saveAsTextFile(objPath)
        println("start save name...")
        nameRdd.saveAsTextFile(namePath)

      }

      val libsvm_data = replaceOldCols(sourceData, objectArray)
      val libsvm_result = label.join(libsvm_data, Seq(key), "left")
      saveTable(libsvm_result, "libsvm_result")
      println("start save libsvm file")
      val libsvm_rdd: RDD[String] = libsvm_result.rdd.map(row => {
          val datum = row.toSeq
          castLibsvmString(datum(1).toString, datum.drop(2))
      })

      libsvm_rdd.saveAsTextFile(libsvmPath)
      indexRdd.saveAsTextFile(indexPath)
      println("save libsvm file finished.")

    }
  }

  def main(args: Array[String]): Unit = {
    //prepareDataTable
    convertLibsvm(args)

  }
}


