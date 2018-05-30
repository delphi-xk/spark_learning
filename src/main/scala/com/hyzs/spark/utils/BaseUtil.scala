package com.hyzs.spark.utils

import java.text.SimpleDateFormat

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Failure, Random, Success, Try}
/**
  * Created by Administrator on 2018/2/5.
  */
object BaseUtil {

  //TODO: implicit conversion
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

  def castTimestampFuc(time:String): Long = {
    getUnixStamp(time).getOrElse(0)
  }

  def getUnixStamp(stamp:String): Option[Long] = stamp match {
    case "" => None
    case _ => Try(dateFormat.parse(stamp).getTime) match {
      case Success(t) => Some(t)
      case Failure(_) => None
    }
  }

  def toDoubleDynamic(x: Any): Double = x match {
    case s: String => s.toDouble
    case num: java.lang.Number => num.doubleValue()
    case _ => throw new ClassCastException("cannot cast to double")
  }

  def anySeqToSparkVector(x: Any): Vector = x match {
    case a: Array[Any] => Vectors.dense(a.map(toDoubleDynamic))
    case s: Seq[Any] => Vectors.dense(s.toArray.map(toDoubleDynamic))
    case r: Row => Vectors.dense(r.toSeq.toArray.map(toDoubleDynamic))
    case v: Vector => v
    case _ => throw new ClassCastException("unsupported class")
  }

  def anySeqToRow(x:Any): Row = x match {
    case a: Array[Any] => Row(a.map(toDoubleDynamic))
    case s: Seq[Any] => Row(s.map(toDoubleDynamic))
    case r: Row => Row(r.toSeq.map(toDoubleDynamic))
    case _ => throw new ClassCastException("unsupported class")
  }

  def readCsvFile(path:String): Array[Array[String]] = {
    val reader = Source.fromFile(path)
    val resBuf = new ArrayBuffer[Array[String]]()
    for(line <- reader.getLines()){
      val cols = line.split(",").map(_.trim)
      resBuf.append(cols)
    }
    resBuf.toArray
  }

  def getGaussionRandom(expectation:Double = 0.0, variance:Double = 1.0, random:Random):Double = {
    random.nextGaussian()* math.sqrt(variance) + expectation
  }

}
