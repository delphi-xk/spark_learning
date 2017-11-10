package com.hyzs.spark.sql

/**
  * Created by Administrator on 2017/9/27.
  */
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

object MergeTable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaSpark")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use hyzs")
    sqlContext.sql("drop table event_swp1")
/*    sqlContext.sql("drop table event_swp2")
    sqlContext.sql("drop table event_swp3")*/

    val data_2000 = sqlContext.sql("select * from event_test_2000")
    val data_4000 = sqlContext.sql("select * from event_test_4000")
    val data_8000 = sqlContext.sql("select * from event_test_8000")

//    val swp_1 = data_2000.join(data_4000, Seq("client_no"), "left_outer")
/*    val swp_2 = data_2000.join(data_8000, Seq("client_no"), "left_outer")
    val swp_3 = data_4000.join(data_8000, Seq("client_no"), "left_outer")*/

//    swp_1.write.saveAsTable("event_swp1")
/*    swp_2.write.saveAsTable("event_swp2")
    swp_3.write.saveAsTable("event_swp3")*/

  }


}
