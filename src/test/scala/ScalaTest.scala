import org.scalatest.FunSuite

import scala.annotation.tailrec
import java.io._

import com.hyzs.spark.ml.ModelEvaluation._
import com.hyzs.spark.utils.BaseUtil
import com.hyzs.spark.utils.BaseUtil._
import org.apache.spark.sql.Row

import scala.io.Source
import scala.util.Random
/**
  * Created by xk on 2018/3/14.
  */
class ScalaTest extends FunSuite{


  def fib(index:Int): BigInt = {
    @tailrec
    def loop(a: BigInt, b: BigInt, idx: Int = 0): BigInt =
      if (idx < index)
        loop(b, a + b, idx + 1)
      else
        a
    loop(0, 1)
  }

  def factorial(index:Int): BigInt = {
    @tailrec
    def loop(a: BigInt, idx:Int): BigInt = {
      if(idx ==0)
        loop(1, 1)
      else if(idx <= index)
        loop(a*idx, idx+1)
      else
        a
    }
    loop(1,0)

  }

  test("generate label test files"){
    val lines = Source.fromFile("d:/feature_data.csv").getLines()
    val ids = lines.map( line => line.split(",").head)
    val writer = new PrintWriter(new File("d:/test_label.txt"))
    writer.write("user_id_md5,label\n")
    ids.foreach{ id =>
      writer.write(s"$id,${Random.nextDouble()}\n")
    }
    writer.close()
  }

  def factorial2(x: Int): BigInt = {
    if(x == 0)1 else x*factorial2(x-1)
  }

  test(" fibonacci test "){
    println(fib(0))
    println(fib(1))
    println(fib(10))
    println(fib(100))
  }


  test(" factorial test "){
    val l = List(1,2,3,45,5)

    println(factorial(0))
    println(factorial(1))
    println(factorial(3))
    println(factorial(5))
  }

  test("Unix time test"){
    val time = "2017-11-11 21:21:56.0"
    val time2 = "2017-11-11 21:21:5"
    println(BaseUtil.getUnixStamp(time))
    println(BaseUtil.getUnixStamp(time2))
  }

  test("matrix test file"){
    val writer = new PrintWriter(new File("d:/test_matrix.txt"))
    for( i <- 0 to 100000){
      for( j <- 0 to 9){
        writer.write((Random.nextDouble()*10)+",")
      }
      writer.write(Random.nextDouble()*10+"\n")
    }
    writer.close()
  }

  test("evaluation test file"){
    val writer = new PrintWriter(new File("d:/evaluation_test.txt"))
    for( i <- 0 until 100000){
      writer.write(i+",")
      writer.write(Random.nextDouble+",")
      writer.write(Random.nextInt(2)+"\n")
    }
    writer.close()
  }

  test("type cast in spark"){
    val row = Row(1,3,4.0, "5")
    for(v <- row.toSeq){
      println(toDoubleDynamic(v))
    }
    println(anySeqToSparkVector(Array(1,2.3,3)))
    println(anySeqToSparkVector(row.toSeq))
  }

  test("test read csv file"){
    val csvFile = readCsvFile("d:/test0515.csv")
    for(i <- 0 until 10){
      println(csvFile(i).mkString(","))
    }
  }

  test("Gaussion random test"){
    generateScores("good_score_test", 0.3,0.01, 0.8,0.01)
    generateScores("bad_score_test", 0.4,1, 0.6,1)
    generateScores("mid_score_test", 0.2,0.5, 0.9,0.5)
  }

  def reformatRandom(num:Double, min:Double = 0.0,max:Double = 1.0):Double = {
    if(num < min) min
    else if(num > max) max
    else num
  }

  def generateScores(fileName:String, e1:Double, v1:Double, e2:Double, v2:Double): Unit ={
    val writer = new PrintWriter(new File(s"d:/$fileName.csv"))
    val random1 = Random
    val random2 = Random
    for(i <- 0 until 100000){
      writer.write(reformatRandom(getGaussionRandom(e1, v1, random1)) +",0\n")
    }
    for(i <- 0 until 1000){
      writer.write(reformatRandom(getGaussionRandom(e2, v2, random2)) +",1\n")
    }
    writer.close()
  }

  test("test RMSE "){
    // data : score, label
    val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("test score====")
    computeRMSE(testData)

    val goodData: Array[(Double, Double)] = readCsvFile("d:/good_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("good score====")
    computeRMSE(goodData)

    val midData: Array[(Double, Double)] = readCsvFile("d:/mid_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("middle score====")
    computeRMSE(midData)

    val badData: Array[(Double, Double)] = readCsvFile("d:/bad_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("bad score====")
    computeRMSE(badData)

  }

  test("test auc"){
    // data : score, label
    //val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
    //val testData: Array[(Double, Double)] = readCsvFile("d:/good_score_test.csv")

    val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("test score====")
    computeAUC(testData)

    val goodData: Array[(Double, Double)] = readCsvFile("d:/good_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("good score====")
    computeAUC(goodData)

    val midData: Array[(Double, Double)] = readCsvFile("d:/mid_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("middle score====")
    computeAUC(midData)

    val badData: Array[(Double, Double)] = readCsvFile("d:/bad_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("bad score====")
    computeAUC(badData)
  }

  test("gaussion dis simu test"){
    generateScores("simu_score_test", 0.15,0.1, 0.9,1.0)
    val testData: Array[(Double, Double)] = readCsvFile("d:/simu_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("simu score test====")
    computeAUC(testData)
    computeRMSE(testData)
  }
}
