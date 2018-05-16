package com.hyzs.spark.ml

import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD

/**
  * Created by xk on 2018/5/8.
  */
object MatrixOpsInSpark extends App{

  val data = sc.textFile("/hyzs/test_data/test_matrix.txt")
  val matrix:RDD[Vector] = data.map( str => {
    val datum = str.split(",").map(field => field.toDouble)
    Vectors.dense(datum)
  })

  val indexedMatrix:RDD[IndexedRow] = matrix.zipWithIndex().map{
    case (vector, index) =>
    IndexedRow(index, vector)
  }

  val rowNum = matrix.count()

  val first = matrix.first()
  val res1 = matrix.map(v => Vectors.sqdist(v, first))


}
