package com.hyzs.spark
import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.sql.functions._
import com.hyzs.spark.utils.BaseUtil._

import scala.collection.mutable.ArrayBuffer
/**
  * Created by xk on 2018/6/13.
  */
object NlpProcess {


  val stopSet:Array[String] = sc.textFile("/test_data/stop_words_zh.txt").collect()
  // srcString is split words with '|' concat
  def removeStopWordFuc(srcString:String): String = {
    val words = srcString.split("\\|")
    val resultArray = new ArrayBuffer[String]()
    for(word <- words){
      if( !(stopSet.contains(word)||isExistDigits(word)) ) resultArray.append(word)
    }
    resultArray.mkString("|")
  }

  def removeStopWord(): Unit ={
    val data = spark.table("nlp.cail2018")
    val stopUdf = udf(removeStopWordFuc _)
    val resData = data.withColumn("words", stopUdf(col("fact")))
    resData.write.saveAsTable("nlp.cail_p1")
  }



}
