package com.hyzs.spark.sql

import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
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

  def findDuplicates(): Unit = {
    val merchants = spark.table("merchant.merchants")
    val count = merchants.select("merchant_id")
      .rdd
      .map(id => (id.getString(0), 1))
      .reduceByKey(_ + _)
      .toDF("merchant_id", "merchant_count")
    merchants.join(count, Seq("merchant_id"), "left").filter("merchant_count > 1")
  }

  def getColumnMode(transData: Dataset[Row], keyColumn:String, modeColumn:String): Dataset[Row] = {
    // generate "count"
    val grouped = transData.groupBy(keyColumn, modeColumn).count()
    // window by key, order by "count"
    val window = Window.partitionBy(keyColumn).orderBy(desc("count"))
    // get max count row
    grouped.withColumn("order", row_number().over(window))
      .filter("order = 1")
      .select(keyColumn, modeColumn)
  }

  def getColumnAgg(transData:Dataset[Row], keyColumn:String, aggColumn:String): Dataset[Row] = {
    transData.groupBy(keyColumn)
      .agg(sum(aggColumn).as(s"sum_$aggColumn"),
        avg(aggColumn).as(s"avg_$aggColumn")
      )
  }

  def transProcess(): Unit = {
    val keyColumn = "card_id"
    val newTrans = spark.table("merchant.new_merchant_transactions")
    var ids = newTrans.select(keyColumn).distinct()
    val processMode  = Seq("city_id", "category_1", "installments", "category_3",
      "merchant_category_id", "category_2", "state_id", "subsector_id")
    for(colName <- processMode){
      val modeTmpTable = getColumnMode(newTrans, keyColumn, colName)
      ids = ids.join(modeTmpTable, Seq(keyColumn), "left")
    }
    val aggTable = getColumnAgg(newTrans, keyColumn, "purchase_amount")
    ids = ids.join(aggTable, Seq(keyColumn), "left")
    saveTable(ids, "new_transactions_processed", "merchant")
  }

  def hisProcess(): Unit = {
    val keyColumn = "card_id"
    val trans = spark.table("merchant.historical_transactions")
    var ids = trans.select(keyColumn).distinct()
    val processMode  = Seq("city_id", "category_1", "installments", "category_3",
      "merchant_category_id", "category_2", "state_id", "subsector_id")
    for(colName <- processMode){
      val modeTmpTable = getColumnMode(trans, keyColumn, colName)
      ids = ids.join(modeTmpTable, Seq(keyColumn), "left")
    }
    val aggTable = getColumnAgg(trans, keyColumn, "purchase_amount")
    ids = ids.join(aggTable, Seq(keyColumn), "left")
    ids = addColumnsPrefix(ids, "historical", Array(keyColumn))
    saveTable(ids, "historical_transactions_processed", "merchant")
  }

  def merchantProcess(): Unit = {
    val keyColumn = "card_id"
    var trainTable = spark.table("merchant.train")
      .select("card_id", "target", "feature_1", "feature_2", "feature_3")
    var testTable = spark.table("merchant.test").withColumn("target", lit(0))
      .select("card_id", "target", "feature_1", "feature_2", "feature_3")
    val transTable = spark.table("merchant.new_transactions_processed")
    val hisTable = spark.table("merchant.historical_transactions_processed")

    trainTable = trainTable.join(transTable, Seq(keyColumn), "left")
        .join(hisTable, Seq(keyColumn), "left")
    saveTable(trainTable, "train_result", "merchant")
    testTable = testTable.join(transTable, Seq(keyColumn), "left")
      .join(hisTable, Seq(keyColumn), "left")
    saveTable(testTable, "test_result", "merchant")
  }

  def main(args: Array[String]): Unit = {
    transProcess()
    hisProcess()
    merchantProcess()
  }

}
