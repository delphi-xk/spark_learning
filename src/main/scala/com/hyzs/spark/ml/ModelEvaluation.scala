package com.hyzs.spark.ml

import com.hyzs.spark.utils.BaseUtil._

import math.BigDecimal
import scala.math.BigDecimal.RoundingMode
/**
  * Created by xk on 2018/5/8.
  */
object ModelEvaluation extends App{

  val globalDecimalScale = 5
  val testData: Array[(BigDecimal, Int)] = readCsvFile("d:/test0515.csv").drop(1)
    .map(row => (getDecimalValue(row(0).toDouble), row(1).toInt))
  val posData = testData.filter(_._2 == 1)
  val negData = testData.filter(_._2 == 0)
  println(posData.length)
  println(negData.length)

  val sortedData = testData.sortBy(_._1)
/*  for(i <- 0 to 20){
    println(sortedData(i))
  }*/
  val threVal = sortedData.map( _._1)
    .distinct
  //println(threVal.length)

/*  for(threshold <- threVal.take(10)){
    println(threshold, countCumNum(threshold, sortedData, posData.length, negData.length, 5))
  }*/

  val pointArray: Array[(BigDecimal, BigDecimal)] = threVal.map( thred => countCumNum(thred, testData))
  println(findKSValue(pointArray))


  def countCumNum(threshold:BigDecimal, data:Array[(BigDecimal, Int)]): (BigDecimal, BigDecimal) = {
    val selectData = data.filter(_._1 <= threshold)
    val posData = selectData.count(_._2 == 1)
    val negData = selectData.count(_._2 == 0)
    val totalPos = data.count(_._2 == 1)
    val totalNeg = data.count(_._2 == 0)
    val posRatio = getDecimalValue(posData) / getDecimalValue(totalPos)
    val negRatio = getDecimalValue(negData) / getDecimalValue(totalNeg)
    (posRatio, negRatio)
  }

/*  def calTrueAndFalsePositive(theshold:BigDecimal, data:Array[(BigDecimal, Int)]): (BigDecimal, BigDecimal) = {
    val posData = data.filter(_._1 >= theshold)
    val negData = data.filter(_._1 < theshold)


  }*/

  def getDecimalValue(num:Double): BigDecimal={
    BigDecimal(num).setScale(globalDecimalScale, RoundingMode.HALF_UP)
  }
  def findKSValue(pointArray:Array[(BigDecimal, BigDecimal)]): BigDecimal = {
    val ksVal = pointArray.map(point => math.abs((point._1 - point._2).toDouble))
    getDecimalValue(ksVal.max)
  }




}
