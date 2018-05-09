package com.hyzs.spark.ml

import org.apache.spark.rdd.RDD
import com.hyzs.spark.utils.SparkUtils._

/**
  * Created by xk on 2018/5/9.
  */
object ModelEvaluation {

  // row( index, label, score)
  def loadDataFromTable(tableName:String): RDD[(Int,Double)] = {
    val scoreRdd = spark.table(tableName).rdd.map(row => (row.getInt(1), row.getDouble(2)))
    scoreRdd
  }

  val scoresRdd:RDD[(Int, Double)] = loadDataFromTable("scores")


}
