package com.hyzs.spark.sql

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Matrices, Vector}
import org.apache.spark.sql.functions._

import com.hyzs.spark.utils.SparkUtils._
/**
  * Created by xiangkun on 2018/6/21.
  */
object JDDataProcess_v2 {

  val key = "phone"





  def processSummaryTable(df:DataFrame): DataFrame = {


    df
  }

  def convertLibsvm(df:DataFrame): Unit ={



  }


}
