package com.hyzs.spark.sql

import com.hyzs.spark.utils.SparkUtils._

/**
  * Created by xk on 2018/3/12.
  */
object NewDataProcess {

  import spark.implicits._

  def importData(): Unit = {
    val features = readCsv("/hyzs/test_data/feature_table.csv")
    saveTable(features, "all_data")
  }

  def main(args: Array[String]): Unit = {
    importData()
  }

}
