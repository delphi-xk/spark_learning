package com.hyzs.spark.ml

import com.hyzs.spark.utils.BaseUtil._

import math.BigDecimal
import scala.math.BigDecimal.RoundingMode
/**
  * Created by xk on 2018/5/8.
  */
object ModelEvaluation extends App{


  val testData: Array[(Double, Int)] = readCsvFile("d:/test0515.csv").drop(1)
    .map(row => (row(0).toDouble, row(1).toInt))
  val posData = testData.filter(_._2 == 1)
  val negData = testData.filter(_._2 == 0)
  println(posData.length)
  println(negData.length)

  val sortedData = testData.sortBy(_._1)
/*  for(i <- 0 to 20){
    println(sortedData(i))
  }*/
  val threVal = sortedData.map(_._1).distinct
  println(threVal.length)

  for(threshold <- threVal.take(10)){
    println(threshold, countCumNum(threshold, sortedData, posData.length, negData.length, 5))
  }

  def countCumNum(threshold:Double, data:Array[(Double, Int)], posNum:Long, negNum:Long, scale:Int): (BigDecimal, BigDecimal) = {
    val selectData = data.filter(_._1 <= threshold)
    val posData = selectData.filter(_._2 == 1)
    val negData = selectData.filter(_._2 == 0)
    val posRatio = (BigDecimal(posData.length) / BigDecimal(posNum)).setScale(scale, RoundingMode.HALF_UP)
    val negRatio = (BigDecimal(negData.length).setScale(scale) / BigDecimal(negNum)).setScale(scale, RoundingMode.HALF_UP)
    (posRatio, negRatio)
  }
}
