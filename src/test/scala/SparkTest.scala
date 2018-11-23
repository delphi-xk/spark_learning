import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, StreamingSuiteBase}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalactic.Equality
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xk on 2018/9/10.
  */



class SparkTest extends FunSuite with SharedSparkContext with RDDComparisons {



  test("test composite key ordering"){
    implicit val compositeOrdering = new Ordering[(String, String)] {
      override def compare(x: (String, String), y: (String, String)) =
        if(x._1.compareTo(y._1) == 0){
          x._2.compareTo(y._2)
        } else x._1.compareTo(y._1)
    }

    val list = List(("A","1"), ("B", "1"),("A","2"), ("B", "7"), ("A","4"), ("B", "3"))
    val rdd:RDD[(String, String)] = sc.parallelize(list)
    val preferOrder = sc.makeRDD(
      List((("A","1"), "1"), (("A","2"), "2"), (("A","4"), "4"), (("B", "1"), "1"), (("B", "3"),"3"), (("B", "7"), "7"))
    )

    val pairRdd: RDD[((String, String), String)] = rdd.map(data => (data, data._2))
    val sortedRdd = pairRdd.sortByKey()
    assertRDDEquals(pairRdd, sortedRdd)
    //assert(None === compareRDDWithOrder(pairRdd, sortedRdd))
    //assertRDDEqualsWithOrder(preferOrder, sortedRdd)

  }

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)
    assert(rdd.count === list.length)
  }

}

class SampleStreamingTest extends FunSuite with StreamingSuiteBase {

  test("simple two stream streaming test") {
    val input = List(List("hi", "pandas"), List("hi holden"), List("bye"))
    val input2 = List(List("hi"), List("pandas"), List("byes"))
    val expected = List(List("pandas"), List("hi holden"), List("bye"))
    testOperation[String, String, String](input, input2, subtract _, expected, ordered = false)
  }

  def subtract(d1: DStream[String], d2: DStream[String]): DStream[String] = {
    d1.transformWith(d2, SampleStreamingTest.subtractRDDs _)
  }

  test("really simple transformation") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))
    testOperation[String, String](input, tokenize _, expected, ordered = false)
  }

  // This is the sample operation we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }

  test("CountByWindow with windowDuration 3s and slideDuration=2s") {
    // There should be 2 windows :  {batch2, batch1},  {batch4, batch3, batch2}
    val batch1 = List("a", "b")
    val batch2 = List("d", "f", "a", "b")
    val batch3 = List("f", "g"," h")
    val batch4 = List("a")
    val input= List(batch1, batch2, batch3, batch4)
    val expected = List(List(6L), List(8L))
    val expected2 = List(List(2L), List(6L), List(7L), List(4L))


    def countByWindow(ds:DStream[String]):DStream[Long] = {
      ds.countByWindow(windowDuration = Seconds(3), slideDuration = Seconds(2))
    }

    def countByWindow2(ds:DStream[String]):DStream[Long] = {
      ds.countByWindow(windowDuration = Seconds(2), slideDuration = Seconds(1))
    }

    testOperation[String, Long](input, countByWindow _, expected, ordered = true)
    testOperation[String, Long](input, countByWindow2 _, expected2, ordered = true)
  }

  test("empty batch by using null") {
    def multiply(stream1: DStream[Int]) = stream1.map(_ * 3)

    val input1 = List(List(1), null, List(10))
    val output = List(List(3), List(30))

    testOperation(input1, multiply _, output, ordered = false)
  }

  test("custom equality object (Integer)") {
    val input = List(List(-1), List(-2, 3, -4), List(5, -6))
    val expected = List(List(1), List(2, 3, 4), List(5, 6))

    implicit val integerCustomEquality =
      new Equality[Int] {
        override def areEqual(a: Int, b: Any): Boolean =
          b match {
            case n: Int => Math.abs(a) == Math.abs(n)
            case _ => false
          }
      }

    def doNothing(ds: DStream[Int]) = ds

    testOperation[Int, Int](input, doNothing _, expected, ordered = false)
    testOperation[Int, Int](input, doNothing _, expected, ordered = true)
  }

  override def maxWaitTimeMillis: Int = 20000

  test("increase duration more than 10 seconds") {
    val input = (1 to 1000).toList.map(x => List(x))
    val expectedOutput = (1 to 1000).toList.map(x => List(2 * x))

    def multiply(ds: DStream[Int]) = ds.map(_ * 2)

    testOperation[Int, Int](input, multiply _, expectedOutput, ordered = true)
  }

}

object SampleStreamingTest {
  def subtractRDDs(r1: RDD[String], r2: RDD[String]): RDD[String] = {
    r1.subtract(r2)
  }
}