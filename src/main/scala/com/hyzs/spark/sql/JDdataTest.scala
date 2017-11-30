package com.hyzs.spark.sql


import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
/**
  * Created by XIANGKUN on 2017/11/27.
  */
object JDdataTest {



  val delimiter = "\\t"
  val conf = new SparkConf().setAppName("DataTest")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sqlContext.setConf("spark.sql.shuffle.partitions", "337")
  sqlContext.setConf("spark.shuffle.file.buffer", "4096k")

  def createDFfromCsv(path: String, delimiter: String = "\\t"): DataFrame = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter))
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  val hisTable = "dmr_rec_s_user_order_amount_one_month"
  val infoTables = List(

    "dmr_rec_v_dmt_upf_s_d_0000017",
    "dmr_rec_v_dmt_upf_s_d_0000030",
    "dmr_rec_v_dmt_upf_s_d_0000034",
    "dmr_rec_v_dmt_upf_s_d_0000035",
    "dmr_rec_v_dmt_upf_s_d_0000056",

    "dmr_rec_v_dmt_upf_s_d_1",
    "dmr_rec_v_dmt_upf_s_d_10",
    "dmr_rec_v_dmt_upf_s_d_2",
    "dmr_rec_v_dmt_upf_s_d_21",
    "dmr_rec_v_dmt_upf_s_d_3",
    "dmr_rec_v_dmt_upf_s_d_34"

//    "dmr_rec_v_dmt_upf_s_d_4",
//    "dmr_rec_v_dmt_upf_s_d_42",
//    "dmr_rec_v_dmt_upf_s_d_44",
//    "dmr_rec_v_dmt_upf_s_d_45",
//    "dmr_rec_v_dmt_upf_s_d_47",
//    "dmr_rec_v_dmt_upf_s_d_48",

//    "dmr_rec_v_dmt_upf_s_d_5",
//    "dmr_rec_v_dmt_upf_s_d_50",
//    "dmr_rec_v_dmt_upf_s_d_51",
//    "dmr_rec_v_dmt_upf_s_d_52",
//    "dmr_rec_v_dmt_upf_s_d_53",
//    "dmr_rec_v_dmt_upf_s_d_55",
//    "dmr_rec_v_dmt_upf_s_d_8",
//    "dmr_rec_v_dmt_upf_s_d_9",
//    "dmr_rec_v_dmt_upf_s_m_56"
  )


  def processHis():Unit = {
    import java.text.SimpleDateFormat
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql._
    import sqlContext.implicits._
    sqlContext.sql("use hyzs")
    val key = "user_id"
    val df1 = sqlContext.sql("select * from dmr_rec_s_user_order_amount_one_month")
    val startDate = "2017-10-21"
    val endDate = "2017-11-19"
    val slotSecs = 86400*7L

    val stampFunc: (String => Int) = (oldDate: String) => {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val oldDateUnix = format.parse(oldDate).getTime / 1000L
      val endDateUnix = format.parse(endDate).getTime / 1000L
      val stamp = Math.floor((endDateUnix - oldDateUnix)/slotSecs)
      stamp.toInt
    }

    val stampUdf = udf(stampFunc)
    val swp = df1
      .withColumn("stamp", stampUdf(col("dt")))
      .groupBy($"user_id",$"stamp").agg(count("user_id").as("count_id"), avg("user_payable_pay_amount").as("avg_pay_amount"))

    val ids = sqlContext.sql("select * from all_user_month")

    var result = ids
    val cols = Seq("count_id", "avg_pay_amount")
    for( index <- 0 to 4){
      val filtered = swp.filter(s"stamp = $index")
        .select(col(key) +:cols.map(name => col(name).as(s"${name}_stamp$index")) : _*)
      result = result.join(filtered, Seq(key), "left_outer")
    }
    sqlContext.sql("drop table order_amount_one_month_new")
    result.write.saveAsTable("order_amount_one_month_new")
  }

  def mergeStage():Unit = {
    val key = "user_id"
    val ids = sqlContext.sql("select * from all_user_month")

    var result_2 = ids
    for(table <- infoTables){
      val t1 = sqlContext.sql(s"select * from $table")
      val t2 = ids.join(t1, Seq("user_id"),"left_outer")
      result_2 = t2.join(result_2,Seq("user_id"),"left_outer")
    }
    //result_2.write.saveAsTable("result_1128_stage1")
    result_2.write.parquet("/hyzs/")
  }

  def mergeResult(): Unit = {
    val resultList = List (
      "order_amount_one_month_new",
      "result_1129_stage1",
      "result_1129_stage2",
      "result_1129_stage3",
      "result_1129_stage4"

    )

    sqlContext.sql("use hyzs")
    val df1 = sqlContext.sql("select * from order_amount_one_month_new")
    val df2 = sqlContext.sql("select * from result_1129_stage1 ")
    val df3 = sqlContext.sql("select * from result_1129_stage2 ")
    val res = df1.join(df2,Seq("user_id"),"left_outer").join(df3,Seq("user_id"), "left_outer")
    sqlContext.sql("drop table union_1130_1")
    res.write.saveAsTable("union_1130_1")




    sqlContext.sql("use hyzs")
    sqlContext.sql("drop table result_1130_2")
    val df4 = sqlContext.sql("select * from union_1130_1")
    val df5 = sqlContext.sql("select * from union_1129_2")
    df4.join(df5,Seq("user_id"),"left_outer").write.saveAsTable("result_1130_2")

  }

  def main(args: Array[String]): Unit = {
    for(table <- infoTables){
      createDFfromCsv(s"/hyzs/data/$table").write.saveAsTable(table)
    }

    //createDFfromCsv(s"/hyzs/data/dmr_rec_v_dmt_upf_s_d_3").write.saveAsTable("dmr_rec_v_dmt_upf_s_d_3")
    //createDFfromCsv(s"/hyzs/data/dmr_rec_v_dmt_upf_s_d_3").write.save("/hyzs/table/dmr_rec_v_dmt_upf_s_d_3")



    //val ids = df1.select("user_id").distinct()
    //ids.write.saveAsTable("all_user_month")

    //df1.groupBy().agg(max("dt"),min("dt"))

    /*    val stampFunc: (String => Int) = (oldDate: String) => {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val oldDateUnix = format.parse(oldDate).getTime / 1000L
      val startDateUnix = format.parse(startDate).getTime / 1000L
      val stamp = Math.floor((oldDateUnix - startDateUnix)/slotSecs)
      stamp.toInt
    }*/

  }

}
