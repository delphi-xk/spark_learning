package com.hyzs.spark.ml


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
/**
  * Created by XIANGKUN on 2017/12/5.
  */
object MLtest {


  val conf = new SparkConf().setAppName("DataTest")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  def convertDFtoLibsvm(): Unit = {
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    val df = sqlContext.sql("select * from consume_train")
    val label = sqlContext.sql("select * from consume_label limit 100")

    val test = label.join(df, Seq("user_id"), "left_outer")
    val rdd_test: RDD[Vector] = test.rdd.map(
      row => Vectors.dense(row.getDouble(0), row.getDouble(2)) )




  }


  def main(args: Array[String]): Unit = {

  }
}
