package com.hyzs.spark.utils

import java.text.SimpleDateFormat

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import scala.util.{Failure, Success, Try}
/**
  * Created by Administrator on 2018/2/5.
  */
object BaseUtil {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")


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

  def anySeqToSparkVector[T](x: Any): Vector = x match {
    case a: Array[T] => Vectors.dense(a.map(toDoubleDynamic))
    case s: Seq[Any] => Vectors.dense(s.toArray.map(toDoubleDynamic))
    case v: Vector => v
    case _ => throw new ClassCastException("unsupported class")
  }

  def anySeqToRow[T](x:Any): Row = x match {
    case a: Array[T] => Row(a.map(toDoubleDynamic))
    case s: Seq[Any] => Row(s.map(toDoubleDynamic))
    case r: Row => Row(r.toSeq.map(toDoubleDynamic))
    case _ => throw new ClassCastException("unsupported class")
  }

}
