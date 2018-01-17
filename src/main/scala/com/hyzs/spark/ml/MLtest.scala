package com.hyzs.spark.ml



import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer
/**
  * Created by XIANGKUN on 2017/12/5.
  */
object MLtest {


  val conf = new SparkConf().setAppName("DataTest")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


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
    val stageBuffer = new ArrayBuffer[PipelineStage]()
    stageBuffer.appendAll(indexerArray.map(_._2))
    val pipeline = new Pipeline()
      .setStages(Array(indexerArray.map(_._2): _*))
      .fit(df)

  }

  def getIndexers(df: DataFrame, col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col, indexer)
  }

  def main(args: Array[String]): Unit = {

  }
}
