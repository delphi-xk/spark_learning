import org.scalatest.FunSuite

import scala.annotation.tailrec
import java.io._

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

}
