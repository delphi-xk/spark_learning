package com.hyzs.spark.ml


import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.mllib.linalg.Vectors
/**
  * Created by xk on 2018/5/8.
  */
object MatrixOps extends App{

  val data = sc.textFile("/hyzs/test_data/test_matrix.txt")
  val matrix = data.map( str => {
    val datum = str.split(",").map(field => field.toDouble)
    Vectors.dense(datum)
  })

  val rowNum = matrix.count()


}
