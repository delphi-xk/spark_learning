package com.hyzs.spark.hbase

import java.util.Random

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}






/**
  * Created by Administrator on 2018/1/3.
  */
object HBaseTest {
  //hbaseConf.set("hbase.master", "master:60000")

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "master")

  val conn = ConnectionFactory.createConnection(hbaseConf)
  val sc = new SparkContext()


  def scanTable(): Unit = {

/*      val tableName = "table1"
      val table = conn.getTable(TableName.valueOf(tableName))
      val g = new Get("1".getBytes)
      val result = table.get(g)
      val value = result.getValue("col1".getBytes,"col2".getBytes).toString
      println(value)
    */

    hbaseConf.set(TableInputFormat.INPUT_TABLE, "table1")
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
    //println("records found:" + hbaseRdd.count())
    val rdd = hbaseRdd.map{
        case (_, result) =>
          val key = Bytes.toString(result.getRow)
          val col1 = Bytes.toString(result.getValue("col1".getBytes, "".getBytes))
          val col2 = Bytes.toString(result.getValue("col2".getBytes, "".getBytes))
          val col3 = Bytes.toString(result.getValue("col3".getBytes, "".getBytes))
          (key,col1,col2,col3)
    }

    //rdd.take(10)
  }

  def createTable(): Unit = {
    val admin = conn.getAdmin
    val userTable = TableName.valueOf("user")
    val tableDesc = new HTableDescriptor(userTable)
    tableDesc.addFamily(new HColumnDescriptor("model1".getBytes))
    tableDesc.addFamily(new HColumnDescriptor("model2".getBytes))
    if (admin.tableExists(userTable)){
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    } else {
      admin.createTable(tableDesc)
    }

  }

  def insertData(): Unit = {
    val userTable = TableName.valueOf("user")
    val table = conn.getTable(userTable)
    val put1 = new Put("id0001".getBytes)
    put1.addColumn("model1".getBytes, "pred1".getBytes, "0.0321".getBytes)
    table.put(put1)

    val put2 = new Put("id0002".getBytes)
    put2.addColumn("model2".getBytes, "pred1".getBytes, "0.03333".getBytes)
    table.put(put2)

  }

  def convertToPut(row: (String, String)) = {
    val put = new Put(Bytes.toBytes(row._1))
    put.addColumn("model1".getBytes, "pred1".getBytes, Bytes.toBytes(row._2))
    (new ImmutableBytesWritable, put)
  }

  def saveData(): Unit = {

    val jobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user")
    val ranGen = new Random
    val rawData = sc.parallelize(
      List(
        ("a",ranGen.nextFloat().toString),
        ("b", ranGen.nextFloat().toString),
        ("c", ranGen.nextFloat().toString),
        ("d",ranGen.nextFloat().toString))
    )
    val localData = rawData.map(convertToPut)
    localData.saveAsHadoopDataset(jobConf)

  }

  def main(args: Array[String]): Unit = {
    saveData()
  }


}
