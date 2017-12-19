/**
  * Created by XIANGKUN on 2017/12/14.
  */

// TODO: add comments
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
val sc = new SparkContext(new SparkConf())
val sqlContext = new SQLContext(sc)

import sqlContext.implicits._
val filePath = sc.getConf.get("spark.predictJob.fileAbsPath")
val tableName = sc.getConf.get("spark.predictJob.file")
println("get pinlist in path: " + filePath)
sqlContext.sql("use hyzs")
val data  = sc.textFile(filePath)
sqlContext.sql(s"drop table hyzs.$tableName")
data.map( pin => pin.toLowerCase)
  .toDF("user_id")
  .write.saveAsTable(s"hyzs.$tableName")
val userList = sqlContext.sql(s"select a.* from result_1205 a, $tableName b where a.user_id = b.user_id")


/**
  *  create risk libsvm data table
  */
sqlContext.sql(s"drop table hyzs.result_risk_test_$tableName")
val riskFilterCols = List("mem_vip_f0001", "mem_vip_f0011", "pay_syt_f0011")
val riskCols = userList.columns diff riskFilterCols
val riskTest = userList.selectExpr(riskCols: _*)
riskTest.write.saveAsTable(s"hyzs.result_risk_test_$tableName")