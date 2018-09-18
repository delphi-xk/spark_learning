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
      writer.write(reformatRandom(getGaussionRandom(e1, v1, random1)) +",\n")
    }
    for(i <- 0 until 2000){
      writer.write(reformatRandom(getGaussionRandom(e2, v2, random2)) +",1\n")
    }
    writer.close()
  }

  def generateRegressionScores(fileName:String, e1:Double, v1:Double, e2:Double, v2:Double): Unit ={
    val writer = new PrintWriter(new File(s"d:/$fileName.csv"))
    val random1 = Random
    val random2 = Random
    for(i <- 0 until 100000){
      writer.write(reformatRandom(getGaussionRandom(e1, v1, random1)) +","
        + reformatRandom(getGaussionRandom(e2, v2, random2)) +"\n")
    }
    writer.close()
  }

  test("test read csv file"){
    val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
      .map(row => (row(0).toDouble, row(1).toDouble))
    val posData = testData.filter( field => field._2 == 1.0)
    val negData = testData.filter( field => field._2 == 0.0)
    println("pos num: "+ posData.length)
    println("neg num: "+ negData.length)

  }

  test("Gaussion random test"){
    generateScores("good_score_test", 0.3,0.1, 0.8,0.1)
    generateScores("mid_score_test", 0.3,0.5, 0.8,0.5)
    generateScores("bad_score_test", 0.4,1, 0.7,1)

  }

  test("regression test file"){
    generateRegressionScores("reg_test1", 0.5, 0.1, 0.8, 0.05)
  }

  test("test regression metric"){
    val data: Array[(Double, Double)] = readCsvFile("d:/reg_test1.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
      .map{ case (d1, d2) =>
      (d1, threshold_func(0.5,d2))
    }
    computeAUC(data)
  }

  def threshold_func(thre:Double, datum:Double): Double = {
    if(datum >= thre) 1.0
    else 0.0
  }


  test("test auc ks rmse"){
    // data : score, label
    //val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
    //val testData: Array[(Double, Double)] = readCsvFile("d:/good_score_test.csv")

    val goodData: Array[(Double, Double)] = readCsvFile("d:/good_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("good score====")
    computeAUC(goodData)
    computeKS(goodData)
    computeRMSE(goodData)

    val midData: Array[(Double, Double)] = readCsvFile("d:/mid_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("middle score====")
    computeAUC(midData)
    computeKS(midData)
    computeRMSE(midData)

    val badData: Array[(Double, Double)] = readCsvFile("d:/bad_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("bad score====")
    computeAUC(badData)
    computeKS(badData)
    computeRMSE(badData)
  }

  test("test data calculation"){
    val testData: Array[(Double, Double)] = readCsvFile("d:/test0515.csv").drop(1)
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("test score====")
    computeAUC(testData)
    computeKS(testData)
    computeRMSE(testData)
  }

  test("gaussion dis simu test"){
    generateScores("simu_score_test", 0.15,0.1, 0.7,0.3)
    val testData: Array[(Double, Double)] = readCsvFile("d:/simu_score_test.csv")
      .map(row => (row(0).toDouble, row(1).toDouble))
    println("simu score test====")
    computeAUC(testData)
    computeKS(testData)
    computeRMSE(testData)
  }

  test("cartesion") {
    val arr0 = List[String]()
    val arr1 = List("a", "b", "c")
    val arr2 = List("d", "e", "f")
    val arr3 = List("g", "h", "i")
    val arr4 = List(arr1, arr2, arr3)
    val res1 = arr1.flatMap { str1: String =>
      arr2.map{ str2:String => List[String](str1, str2) }
    }
//    println(res1)

    val res2 = arr4.foldLeft(List[List[String]]()){ (cumArr:List[List[String]],addArr:List[String]) =>
      if(cumArr.isEmpty && addArr.nonEmpty) addArr.map{ t2:String => List(t2) }
      else if (cumArr.nonEmpty && addArr.isEmpty) cumArr
      else cumArr.flatMap{ t1:List[String] => addArr.map{t2:String => t1 :+ t2 } }
    }
//    println(res2)

  }


  def cartesion_product(arrs:List[List[String]]): List[List[String]] = {
    arrs.foldLeft(List[List[String]]()){ (cumArr:List[List[String]],addArr:List[String]) =>
      if(cumArr.isEmpty && addArr.nonEmpty) addArr.map{ t2:String => List(t2) }
      else if (cumArr.nonEmpty && addArr.isEmpty) cumArr
      else cumArr.flatMap{ t1:List[String] => addArr.map{t2:String => t1 :+ t2 } }
    }
  }

  test("test cartesion 2"){
    val str_line = "a,b,c|d,e|f,g"
    val row:List[String] = str_line.split("\\|").toList
    val datum:List[List[String]] = row.map(ele => ele.split(",").toList)
    println(datum)
    val res = cartesion_product(datum)
    println(res)
  }

}
