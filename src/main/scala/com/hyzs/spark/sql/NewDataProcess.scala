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


  def accumulator():Unit = {
    val accum = sc.longAccumulator("my counter")
    sc.parallelize(0 to 10).foreach{x =>
      println(accum.value)
      accum.add(x)
    }
  }

  def importData(): Unit = {
    val features = readCsv("/hyzs/test_data/feature_table.csv")
    saveTable(features, "jd_test_data")
  }

  def importLabel(): Unit = {
    val features = readCsv("/hyzs/test_data/test_label.txt")
    saveTable(features, "jd_test_label")
  }

  def importOldData(): Unit = {
    val table = createDFfromBadFile(headerPath=headerPath, dataPath=dataPath, logPath = tableName)
    saveTable(table, "all_data")
  }

  def main(args: Array[String]): Unit = {
    importLabel()
    importData()
  }

}
