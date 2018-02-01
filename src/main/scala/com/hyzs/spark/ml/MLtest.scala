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
import com.hyzs.spark.utils.InferSchema
import scala.collection.mutable.ArrayBuffer

/**
  * Created by XIANGKUN on 2017/12/5.
  */
object MLtest {


  val conf = new SparkConf().setAppName("DataTest")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
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
    (col, indexer)
  }

  // TODO: row type cast based on castArray
  def castRowType(row: Row, castArray: Seq[DataType]): Row = {
    assert(row.length == castArray.length)
    val rowData = row.toSeq.zip(castArray)
    val newRow = rowData.map{ case (datum, dType) =>
      val newData = (datum, dType) match {
        case ("", _) => ""
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
    val transformed = indexer.transform(df).drop(col).withColumn(col, df(s"${col}_indexer"))
    (transformed, indexer)
  }

  def castDFdtype(df:DataFrame, colName:String, dType:DataType): DataFrame = {
    assert(df.columns contains colName)
    val indexerArray = new ArrayBuffer[StringIndexerModel]
    val df_new = dType match {
      case StringType => {
        val (res, indexer) = castStringType(df, colName)
        indexerArray.append(indexer)
        res
      }
      case TimestampType => df.withColumn(s"$colName", unix_timestamp(df(s"$colName")))
      case _ => df.withColumn(s"$colName", df(s"$colName").cast(DoubleType))
    }
    df_new
  }


  def main(args: Array[String]): Unit = {
    val df = sqlContext.table("test.jd_test_data").drop(originalKey)

    // StructType = Seq[StructField]
    val schema = InferSchema.inferSchema(df)

    val index = df.select(key)
    val data = df.drop(key)




  }
}
