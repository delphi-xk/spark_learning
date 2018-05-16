package com.hyzs.spark.sql

import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
/**
  * Created by xk on 2018/3/12.
  */
object NewDataProcess {

  import spark.implicits._


  val tableName = "all_data"
  val basePath = "/hyzs/test_data/"


  def accumulator():Unit = {
    val accum = sc.longAccumulator("my counter")
    sc.parallelize(0 to 10).foreach{x =>
      println(accum.value)
      accum.add(x)
    }
  }

  def importData(): Unit = {
    val features = readCsv("/hyzs/test_data/feature_table.csv", ",")
    saveTable(features, "jd_test_data")
  }

  def importLabel(): Unit = {
    val features = readCsv("/hyzs/test_data/test_label.txt", ",")
    saveTable(features, "jd_test_label")
  }

  def importOldData(): Unit = {
    val table = createDFfromBadFile(headerPath=s"${basePath}feature_header.csv",
      dataPath=s"${basePath}feature_data4.csv",
      dataSplitter="\002",
      logPath = tableName)
    saveTable(table, "all_data")
  }

  def importOldData2():Unit = {
    val table = createDFfromSeparateFile(headerPath=s"${basePath}feature_header.csv",
      dataPath=s"${basePath}feature_data3.csv", dataSplitter = "\002")
    saveTable(table, "all_data")
  }

  def importNewData(): Unit = {
    val table1 = readCsv("/hyzs/test_data/jrlab_hyzs_sample_n_enc.txt", "\t")
    val table2 = readCsv("/hyzs/test_data/jrlab_hyzs_sample_ord_enc.txt", "\t")
    val table3 = readCsv("/hyzs/test_data/jrlab_hyzs_sample_w_enc.txt", "\t")

    table1.write.saveAsTable("sample_n_enc")
    table2.write.saveAsTable("sample_ord_enc")
    table3.write.saveAsTable("sample_w_enc")
    //table1.orderBy("id")
  }

  // 1 until length merge
  def mergeArray(arr1:Array[String], arr2:Array[String]): Array[String] = {
    assert(arr1.length == arr2.length)
    for(i <- 1 until arr1.length){
      if(arr2(i) != "") arr1(i) = arr2(i).toString
    }
    arr1
  }

  def columnToRow(table:Dataset[Row]): Dataset[Row] = {
    val tags = table.select("tag_cd")
      .distinct()
      .map(row => row.getString(0))
      .collect()
      .sorted

    val tagMap = tags.zip(1 to tags.length).toMap

    val resRdd = table.rdd
      .map{ row =>
        val res = Array.fill[String](tags.length+1)("")
        res(0) = row.getString(0)
        val index = tagMap(row.getString(1))
        res(index) = row.getString(2)
        res
      }
      .keyBy(row => row(0))
      .reduceByKey(mergeArray)
      .map(tuple => Row(tuple._2: _*))
    val schema = StructType(("id"+:tags).map(tag => StructField(tag, StringType)))
    val resTable = spark.createDataFrame(resRdd, schema)
    resTable
  }

  def preprocessOrder(): Unit = {
    val sample_order = spark.table("sample_ord_enc")
    sample_order.groupBy("id")
      .agg(count("id").as("count_id"),
        avg("no_prefr_total_amount").as("avg_prefr_amount"),
        avg("user_actual_pay_amount").as("avg_pay_amount"))
      .selectExpr("id",
        "cast (count_id as string) count_id",
        "cast (avg_prefr_amount as string) avg_prefr_amount",
        "cast (avg_pay_amount as string) avg_pay_amount")
    saveTable(sample_order, "sample_order")
  }

  def main(args: Array[String]): Unit = {
    //preprocessOrder()
    val key = "id"
    val table = spark.table("sample_n_enc")
    val sample1 = spark.table("sample_w_enc")
    val rowTable = columnToRow(table)
    saveTable(rowTable, "sample_2")

    val sample_features = sample1.join(table, Seq("id"), "left")
    saveTable(sample_features, "sample_features")

    val all = spark.table("hyzs.all_data")

    //val diffCols = all.columns diff sample_features.columns

    val order = spark.table("sample_order")
    //spark.sql("drop table sample_fix")
    //spark.sql("create table sample_fix(id string, brs_brs_p0001308 string, mkt_schd_p0001328 string, mkt_schd_p0001327 string)")

    val fix = spark.table("sample_fix")
    val features = sample_features.join(fix, Seq("id"), "left")
      .selectExpr("id"+:(all.columns diff Seq("user_id", "user_id_md5")): _*)
    saveTable(features, "features")
    val sample_all = order.join(features, Seq("id"), "right")
    saveTable(sample_all, "sample_all")
    sample_all
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/hyzs/test_data/sample_all.csv")
  }

}
