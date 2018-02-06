package com.hyzs.spark.utils

import java.text.SimpleDateFormat
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




}
