package com.hyzs.spark.ml

import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.BaseUtil._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by xk on 2018/5/8.
  */
object MatrixOpsInSpark {

  def kmeansTest(): Unit ={
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


}
