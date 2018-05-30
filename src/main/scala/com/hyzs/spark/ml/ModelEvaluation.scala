package com.hyzs.spark.ml

import java.io._

import com.hyzs.spark.mllib.evaluation.ModelSummarizer
import com.hyzs.spark.utils.BaseUtil._
import org.apache.spark.mllib.linalg.Vectors

import math.BigDecimal
import scala.math.BigDecimal.RoundingMode
/**
  * Created by xk on 2018/5/8.
  */
object ModelEvaluation extends App{


  def computeRMSE(testData:Array[(Double,Double)]): Unit ={
    val summary = testData.map{ case (score, label) => Vectors.dense(label, label-score)}
      .aggregate(new ModelSummarizer())(
        (summary, v) => summary.add(v),
        (sum1, sum2) => sum1.merge(sum2)
      )

    val SSerr = math.pow(summary.normL2(1), 2)
    val rmse = math.sqrt(SSerr / summary.count)
    //println("error variance:"+summary.variance(1))
    //println("error mean:"+summary.mean(1))
    //println("mean absolute error:"+ (summary.normL1(1)/ summary.count))
    //println("SSerr:"+SSerr)
    println("rmse:"+rmse)
  }

  def computeKS(testData:Array[(Double,Double)]): Unit ={
    val simpleData = simplifyPointByScale(testData, 2)
    val sortedData = simpleData.sortBy(_._1)
    val threVal = sortedData.map( _._1).distinct

    val ksPointArray: Array[(Double, Double)] = threVal
      .map( thred => countCumulativeRatio(thred, testData))
    println(s"ks value: "+ findKSValue(ksPointArray))

    //val ksPoints1: Array[(Double, Double)] = threVal.zip(ksPointArray.map(_._1))
    //val ksPoints2: Array[(Double, Double)] = threVal.zip(ksPointArray.map(_._2))
    //outputPointArray("d:/ks-points1.csv", simplifyPointByScale(ksPoints1))
    //outputPointArray("d:/ks-points2.csv", simplifyPointByScale(ksPoints2))
  }

  def computeAUC(testData:Array[(Double,Double)]): Unit ={
    val simpleData = simplifyPointByScale(testData, 3)
    val sortedData = simpleData.sortBy(_._1)
    val threVal = sortedData.map( _._1).distinct

    // from 0 to 1
    val rocPointArray: Array[(Double, Double)] = threVal
      .map( thred => countTrueAndFalsePositiveRatio(thred, testData))
      .reverse
    val simplePoints = simplifyPointByScale(rocPointArray, 3)
    // print points
/*    for( point <- simplePoints){
      println(point)
    }*/

    println(s"area under curve: ${areaUnderCurve(simplePoints)}")
    //outputPointArray("d:/roc-simple-points.csv", simplePoints)
  }

  // calculate cumulative ratio for ks test
  def countCumulativeRatio(threshold:Double, data:Array[(Double, Double)]): (Double, Double) = {
    val selectData = data.filter(_._1 <= threshold)
    val posData = selectData.count(_._2 == 1)
    val negData = selectData.count(_._2 == 0)
    val totalPos = data.count(_._2 == 1)
    val totalNeg = data.count(_._2 == 0)
    val posRatio = (getDecimalValue(posData) / getDecimalValue(totalPos)).toDouble
    val negRatio = (getDecimalValue(negData) / getDecimalValue(totalNeg)).toDouble
    (negRatio, posRatio)
  }

  // calculate ratio for roc curve, TPR = TP / P, FPR = FP / N
  def countTrueAndFalsePositiveRatio(theshold:Double, data:Array[(Double, Double)]): (Double, Double) = {
    val totalPos = data.count(_._2 == 1)
    val totalNeg = data.count(_._2 == 0)
    val TP = data.count(datum => datum._1 >= theshold && datum._2 == 1)
    val FP = data.count(datum => datum._1 >= theshold && datum._2 == 0)
    val TPR = (getDecimalValue(TP.toDouble) / getDecimalValue(totalPos)).toDouble
    val FPR = (getDecimalValue(FP.toDouble) / getDecimalValue(totalNeg)).toDouble
    (FPR, TPR)
  }

  // return decimal value in scale
  def getDecimalValue(num:Double, scale:Int = 5): BigDecimal={
    BigDecimal(num).setScale(scale, RoundingMode.HALF_UP)
  }

  // find ks value: max absolute distance
  def findKSValue(pointArray:Array[(Double, Double)]): Double = {
    val ksVal = pointArray.map(point => math.abs(point._1 - point._2))
    ksVal.max
  }

  // remove points by scale
  def simplifyPointByScale(pointArray:Array[(Double, Double)],
                           scale:Integer = 2): Array[(Double, Double)] = {
    pointArray.map(datum =>
      (getDecimalValue(datum._1, scale), getDecimalValue(datum._2, scale)) ).distinct
      .map(datum => (datum._1.toDouble,datum._2.toDouble) )
  }

  // (0., 0.) , (x_1, y_1) , ... (1. , 1.)
  def areaUnderCurve(data:Array[(Double, Double)]): Double = {
    val part1: Array[(Double, Double)] = (0.0, 0.0) +: data
    val part2: Array[(Double, Double)] = data :+ (1.0, 1.0)
    part1.zip(part2)
      .map(pairs => areaUnderTwoPoint(pairs._1._1, pairs._1._2,pairs._2._1, pairs._2._2))
      .sum
  }

  def areaUnderTwoPoint(x1:Double, y1:Double, x2:Double, y2:Double): Double = {
    if(x1 != x2){
      (y1 + y2) * math.abs(x1 - x2) / 2
    } else 0
  }

  def outputPointArray(outputPath:String, pointArray:Array[(Double, Double)]): Unit ={
    val writer = new PrintWriter(new File(outputPath))
    for(point <- pointArray){
      writer.write(s"${point._1},${point._2}"+"\n")
    }
    writer.close()
  }

}
