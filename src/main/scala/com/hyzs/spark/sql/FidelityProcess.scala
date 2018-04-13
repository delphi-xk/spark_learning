package com.hyzs.spark.sql

import java.text.{ParseException, SimpleDateFormat}

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import com.hyzs.spark.utils.SparkUtils.checkHDFileExist
import com.hyzs.spark.utils.SparkUtils.dropHDFiles

/**
  * Created by xiangkun on 2018/4/12.
  */
object FidelityProcess extends App{

  val warehouseDir = "/user/hive/warehouse/"
  def saveTable(df: DataFrame, tableName:String, dbName:String="default"): Unit = {
    sqlContext.sql(s"drop table if exists $dbName.$tableName")
    val path = s"$warehouseDir$tableName"
    if(checkHDFileExist(path))dropHDFiles(path)
    df.write
      .option("path",path)
      .saveAsTable(s"$dbName.$tableName")
  }

  val conf = new SparkConf().setAppName("FidelityProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._
  val key = "ACCOUNT_NO"
  val tables = Seq("rt_account_cbr_details", "rt_account_details",
    "rt_account_scheme_details", "rt_member_account_details")

  val t1 = sqlContext.table("rt_account_cbr_details")
  val t2 = sqlContext.table("rt_account_details")
  val t3 = sqlContext.table("rt_account_scheme_details")
  val t4 = sqlContext.table("rt_member_account_details")
  val t5 = sqlContext.table("rt_awd_details_1")
  val trans = sqlContext.table("rt_transaction")

  // generate ids table
  val tmp1 = t3.filter(col("SCHEME_CODE") === "FMPF").select("ACCOUNT_NO")
  val tmp2 = t2.select("ACCOUNT_NO", "MEMBER_ACCOUNT_NO")
  val tmp3 = t4.select("MEMBER_ACCOUNT_NO", "REAL_ID")
  var ids = tmp1.join(tmp2, Seq("ACCOUNT_NO")).join(tmp3, Seq("MEMBER_ACCOUNT_NO"))
  saveTable(ids, "account_ids")

  // process transaction table -----
  def stampFunc(startDate:String): Int = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var startDateUnix = 0L
    val endDate = "2017-12-31 23:59:59"
    val slotSecs = 86400*30L
    try{
      startDateUnix = format.parse(startDate).getTime / 1000L
    } catch {
      case e: ParseException => startDateUnix=0L
    }
    val endDateUnix = format.parse(endDate).getTime / 1000L
    val stamp = Math.floor((endDateUnix - startDateUnix) / slotSecs)
    stamp.toInt
  }

  val stampUdf = udf(stampFunc _)
  val swp = trans.withColumn("COUNT_DATE",
    when($"DEAL_DATE_REG" !== "", $"DEAL_DATE_REG").otherwise($"DEAL_DATE_INIT"))
    .withColumn("COUNT_VALUE", $"EME_REG_CBR" + $"EMR_REG_CBR" + $"EMR_INIT_CBR" + $"EME_INIT_CBR")
    .filter("COUNT_DATE <= '2017-12-31 23:59:59'")
    .withColumn("stamp", stampUdf($"COUNT_DATE"))
    .filter("stamp <= 12")
    .groupBy(col(key), col("stamp"))
    .agg(count($"ACCOUNT_NO").as("COUNT_NO"), sum($"COUNT_VALUE").as("SUM_VALUE"))

  var trans_result = swp.select(key).distinct
  for(s <- 0 until 12){
    val s_tmp = swp.filter($"stamp" === s)
      .select(col(key), col("COUNT_NO").as("COUNT_NO_STAMP"+s), col("SUM_VALUE").as("SUM_VALUE_STAMP"+s))
    trans_result = trans_result.join(s_tmp, Seq(key), "left")
  }
  saveTable(trans_result, "rt_transaction_summary")

  // join all table
  val keyList = Seq("ACCOUNT_NO", "MEMBER_ACCOUNT_NO")
  val summary = sqlContext.table("rt_transaction_summary").repartition(500, col("ACCOUNT_NO"))

  def preprocessTable(tableName:String, key:String): DataFrame = {
    val table = sqlContext.table(tableName)
    table.dropDuplicates(Seq(key))
      .select(
        table.columns
          .map(field =>
            if(keyList contains field)col(field)
            else col(field).as(s"${tableName}__$field")): _*)
      .repartition(500, col(key))
  }

  val s1 = preprocessTable("rt_account_cbr_details", "ACCOUNT_NO")
  val s2 = preprocessTable("rt_account_details", "ACCOUNT_NO")
  val s3 = preprocessTable("rt_account_scheme_details", "ACCOUNT_NO")
  val s4 = preprocessTable("rt_member_account_details", "MEMBER_ACCOUNT_NO")
  ids = ids.repartition(500, col("ACCOUNT_NO"))
  val join_result = ids.join(s1, Seq("ACCOUNT_NO"), "left")
    .join(s2, Seq("ACCOUNT_NO"), "left")
    .join(s3, Seq("ACCOUNT_NO"), "left")
    .join(summary, Seq("ACCOUNT_NO"), "left")
    .repartition(500, col("MEMBER_ACCOUNT_NO"))
    .join(s4, Seq("MEMBER_ACCOUNT_NO"), "left")
  saveTable(join_result, "all_data")

  // generate label table
  // rt_account_details__HSBC_PIN_STATUS, rt_account_details__ACTIVE_FLAG, rt_account_cbr_detilas__TERMINATION_DATE
  val accounts = sqlContext.table("account_ids")
  val tag_member = t4.where(
    $"WORK_TYPE".isin("TRFOUT", "INTRAGPOUT")
      and $"WORK_STATUS".isin("TTCHECKED", "CONFIRMED", "HOLDINGTRF"))
    .select("MEMBER_ACCOUNT_NO").distinct
  val tag_account = trans.where($"TRANSACTION_TYPE" === "X5")
    .select("ACCOUNT_NO").distinct()
  val labels = accounts.join(tag_member, Seq("MEMBER_ACCOUNT_NO"), "inner")
    .join(tag_account, Seq("ACCOUNT_NO"), "inner")
  val positives = labels.select("ACCOUNT_NO").withColumn("label", lit(1))
  saveTable(positives, "positive_labels")

  val all_label = ids.join(positives, Seq("ACCOUNT_NO"), "left")
    .na.fill(0)
  saveTable(all_label, "all_labels")

  // generate train data
  // use databricks csv jar: --jars commons-csv-1.1.jar,spark-csv_2.10-1.5.0.jar
  val data = sqlContext.table("all_data").drop("MEMBER_ACCOUNT_NO")
  val strCol = data.columns.map( field => s"cast ($field as string) $field")
  val train = data.selectExpr(strCol: _*)
    .na.fill("\\N")
    .na.replace("*", Map("null" -> "\\N", "NULL" -> "\\N", "" -> "\\N"))
  // note multiple partitions will produce multiple header line
  train.repartition(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("data")
  val label = sqlContext.table("all_label")
  label.repartition(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("label")


}
