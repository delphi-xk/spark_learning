package com.hyzs.spark.ml

import java.io._
import com.hyzs.spark.utils.BaseUtil._

import math.BigDecimal
import scala.math.BigDecimal.RoundingMode
/**
  * Created by xk on 2018/5/8.
  */
object ModelEvaluation extends App{

  // data : score, label
  val testData: Array[(Double, Int)] = readCsvFile("d:/test0515.csv").drop(1)
    .map(row => (row(0).toDouble, row(1).toInt))
  val sortedData = testData.sortBy(_._1)
  val threVal = sortedData.map( _._1).distinct

  val ksPointArray: Array[(Double, Double)] = threVal
    .map( thred => countCumulativeRatio(thred, testData))
  println(s"ks value: "+ findKSValue(ksPointArray))

  val ksPoints1: Array[(Double, Double)] = threVal.zip(ksPointArray.map(_._1))
  val ksPoints2: Array[(Double, Double)] = threVal.zip(ksPointArray.map(_._2))
  //outputPointArray("d:/ks-points1.csv", simplifyPointByScale(ksPoints1))
  //outputPointArray("d:/ks-points2.csv", simplifyPointByScale(ksPoints2))


  // from 0 to 1
  val rocPointArray: Array[(Double, Double)] = threVal
    .map( thred => countTrueAndFalsePositiveRatio(thred, testData))
    .reverse
  val simplePoints = simplifyPointByScale(rocPointArray, 3)
  //val simplePoints = Array( (0.0, 0.0), (0.05, 0.6))
  for( point <- simplePoints){
    println(point)
  }
  println(s"area under curve: ${areaUnderCurve(simplePoints)}")
  //outputPointArray("d:/roc-simple-points.csv", simplePoints)


  def countCumulativeRatio(threshold:Double, data:Array[(Double, Int)]): (Double, Double) = {
    val selectData = data.filter(_._1 <= threshold)
    val posData = selectData.count(_._2 == 1)
    val negData = selectData.count(_._2 == 0)
    val totalPos = data.count(_._2 == 1)
    val totalNeg = data.count(_._2 == 0)
    val posRatio = (getDecimalValue(posData) / getDecimalValue(totalPos)).toDouble
    val negRatio = (getDecimalValue(negData) / getDecimalValue(totalNeg)).toDouble
    (negRatio, posRatio)
  }


  // TPR = TP / P, FPR = FP / N
  def countTrueAndFalsePositiveRatio(theshold:Double, data:Array[(Double, Int)]): (Double, Double) = {
    val totalPos = data.count(_._2 == 1)
    val totalNeg = data.count(_._2 == 0)
    val TP = data.count(datum => datum._1 >= theshold && datum._2 == 1)
    val FP = data.count(datum => datum._1 >= theshold && datum._2 == 0)
    val TPR = (getDecimalValue(TP.toDouble) / getDecimalValue(totalPos)).toDouble
    val FPR = (getDecimalValue(FP.toDouble) / getDecimalValue(totalNeg)).toDouble
    (FPR, TPR)
  }

  def getDecimalValue(num:Double, scale:Int = 5): BigDecimal={
    BigDecimal(num).setScale(scale, RoundingMode.HALF_UP)
  }
  def findKSValue(pointArray:Array[(Double, Double)]): Double = {
    val ksVal = pointArray.map(point => math.abs(point._1 - point._2))
    ksVal.max
  }

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
    if(x1 != x2 && y1 != y2){
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
