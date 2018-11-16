package com.hyzs.spark.sql

import java.text.{ParseException, SimpleDateFormat}

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, SaveMode}
import com.hyzs.spark.utils.SparkUtils._
/**
  * Created by xk on 2018/11/16.
  */
object FilProcess extends App{

  import sqlContext.implicits._
  var endDate = "2018-09-01 00:00:00"
  if(args.length >0) endDate = FilProcess.args(0)
  println("end date: " + endDate)

  def stampFunc(endDate:String)(startDate:String): Int = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var startDateUnix = 0L
    val slotSecs = 86400*30L
    if(startDate != null){
      try{
        startDateUnix = format.parse(startDate).getTime / 1000L
      } catch {
        case e: ParseException => startDateUnix=0L
      }
    }
    val endDateUnix = format.parse(endDate).getTime / 1000L
    val stamp = Math.floor((endDateUnix - startDateUnix) / slotSecs)
    stamp.toInt
  }

  val stampUdf = udf(stampFunc(endDate) _)

  val data = spark.table("test_convert_2w")
  val result = data.select("phone", "jdmall_user_p0008")
    .withColumn("stamp", stampUdf($"jdmall_user_p0008"))
  saveTable(result, "test_udf_result")

}
