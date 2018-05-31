package com.hyzs.spark.ml

import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.BaseUtil._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
/**
  * Created by xk on 2018/5/8.
  */
object MatrixOpsInSpark {


  def importDataset(): Unit ={
    val data: Dataset[Row] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "False")
      .load("/test_data/test_matrix.txt")

    val rdd = data.rdd.map{ row =>
      Row(Vectors.dense(row.toSeq.toArray.map(toDoubleDynamic)))
    }

    val schema = StructType(Seq(
      StructField("features", VectorType)
    ))

    val dataset:Dataset[Row] = spark.createDataFrame(rdd, schema)
    dataset.write.saveAsTable("vector_test")
  }

  def kmeansTest(): Unit ={
    val dataset = spark.table("vector_test")
    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }

  def logisticRegressionTest(): Unit ={
    val training = spark.table("kddcup_vector")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel:LogisticRegressionModel = lr.fit(training)
    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    roc.show()
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

  }

  def convertDataSetToLabeledPoint(dataSet:Dataset[Row]): Dataset[LabeledPoint] = {
    val labeled = dataSet.map{ row =>
      val datum:Array[Double] = row.toSeq.map(toDoubleDynamic).toArray
      val labeledPoint = LabeledPoint(datum(0), Vectors.dense(datum.drop(1)))
      labeledPoint
    }
    labeled
  }

}
