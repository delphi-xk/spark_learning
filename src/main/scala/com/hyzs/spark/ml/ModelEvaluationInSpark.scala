package com.hyzs.spark.ml

import com.hyzs.spark.ml.evaluation.{BinaryConfusionMatrix, BinaryConfusionMatrixImpl, BinaryLabelCounter}
import org.apache.spark.rdd.RDD
import com.hyzs.spark.utils.SparkUtils._
import com.hyzs.spark.utils.BaseUtil._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
/**
  * Created by xk on 2018/5/9.
  */
object ModelEvaluationInSpark {
  val threshold = 0.5

  // src row(index, prediction, label), result row(prediction, label)
  def loadDataFromTable(tableName:String): RDD[Row] = {
    val scoreRdd = spark.table(tableName).rdd.map(row => anySeqToRow(Seq(row(1), row(2))))
    scoreRdd
  }

  // src row(score, label), result row(score, label, pred_label)
  def getLabeledRDD(threshold:Double, rdd:RDD[Row]): RDD[Row] ={
    val labeledRdd = rdd.map( row => {
      val score = toDoubleDynamic(row(0))
      if(score >= threshold) Row(row.toSeq :+ 1.0)
      else Row(row.toSeq :+ 0.0)
    })
    labeledRdd
  }

  def getConfusionMatrix(threshold:Double, labeledRdd:RDD[Row]): BinaryConfusionMatrix = {
    val posNum = labeledRdd.filter(row => row.getDouble(1) == 1.0).count()
    val negNum = labeledRdd.filter(row => row.getDouble(1) == 0.0).count()
    val truePosNum = labeledRdd.filter(row => row.getDouble(1) == 1.0 && row.getDouble(2) == 1.0).count()
    val falsePosNum = labeledRdd.filter(row => row.getDouble(1) == 0.0 && row.getDouble(2) == 1.0).count()
    val posCount = new BinaryLabelCounter(truePosNum, falsePosNum)
    val totalCount = new BinaryLabelCounter(posNum, negNum)
    val confusion = BinaryConfusionMatrixImpl(posCount, totalCount)
    confusion
  }

  val scoresRdd:RDD[Row] = loadDataFromTable("scores")
  val predictionsAndLabels:RDD[(Double, Double)] = scoresRdd.map(
    row => (toDoubleDynamic(row(0)), toDoubleDynamic(row(1))) )
  val predictRdd:RDD[Row] = getLabeledRDD(threshold, scoresRdd)

  val metrics = new BinaryClassificationMetrics(predictionsAndLabels)

  val precision = metrics.precisionByThreshold
  val auc = metrics.areaUnderROC()

  // Get the RMSE using regression metrics
  val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
  println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

}

