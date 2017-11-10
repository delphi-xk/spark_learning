package com.hyzs.spark.sql

import java.io.File
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/11/7.
  */
object SparkTest {
  val conf = new SparkConf().setMaster("local").setAppName("SparkLocalTest")
  val sc = new SparkContext(conf)

  def startLocal(): Unit = {
    val filePath = "d:/workspace/output"
    delPath(new File(filePath))


    val buffer = mutable.ListBuffer[String]()

    buffer += "aaa bbb ccc"
    buffer += "111 aaa 333 bbb 55"
    val flatRdd = sc.parallelize(buffer.toList)
      .flatMap(line => line.split(" "))
    val reduceRdd = flatRdd.map(word => (word,1))
      .reduceByKey(_+_)
    reduceRdd.saveAsTextFile(filePath)

  }

  def groupByTest(): Unit = {
    val args = Seq[String]()
    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    var valSize = if (args.length > 2) args(2).toInt else 1000
    var numReducers = if (args.length > 3) args(3).toInt else numMappers

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random

      // map output sizes lineraly increase from the 1st to the last
      numKVPairs = (1.0 * (p + 1) / numMappers * numKVPairs).toInt

      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(numKVPairs), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    //pairs1.foreach(println)

    println("test output:"+pairs1.groupByKey(numReducers).count())

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    //startLocal()
    groupByTest()
  }


  def delPath(file:File): Unit = {
    if(!file.exists()){
      return
    } else if (file.isFile){
      file.delete()
      println("file is deleted"+file)
    } else if (file.isDirectory){
      val files = file.listFiles()
      for(single <- files){
        delPath(single)
      }
      file.delete()
      println("directory is delete"+file)
    }

  }

}
