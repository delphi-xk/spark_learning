package com.hyzs.spark.sql

import com.hyzs.spark.utils.SparkUtils._

/**
  * Created by xk on 2018/3/12.
  */
object NewDataProcess {

  import spark.implicits._

  val headerPath = "/hyzs/test_data/feature_header.csv"
  val dataPath = "/hyzs/test_data/feature_data2.txt"
  val tableName = "all_data"

  def importData(): Unit = {
    val features = readCsv("/hyzs/test_data/feature_table.csv")
    saveTable(features, "all_data")
  }

  def importOldData(): Unit = {
    val table = createDFfromBadFile(headerPath=headerPath, dataPath=dataPath, logPath = tableName)
    saveTable(table, "all_data")
  }

  def main(args: Array[String]): Unit = {
    importOldData()
  }

}
