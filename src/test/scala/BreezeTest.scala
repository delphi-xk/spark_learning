import org.scalatest.FunSuite

/**
  * Created by xk on 2018/6/1.
  */

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._
import breeze.stats.DescriptiveStats._
import breeze.stats._

class BreezeTest extends FunSuite{



  test("test breeze algebra"){
    val v1 = DenseVector.zeros[Double](5)
    println(v1)
    v1(1 to 2) := .5
    println(v1)
    v1(0 to 1) := DenseVector(.1,.2)
    println(v1)
    val m = DenseMatrix.zeros[Int](5,5)

    val v2 = DenseVector(.1, .2, .3, .4, .5)
    val m2 = DenseMatrix(v1, v2)
    m2(0 to 1, 0 to 2) := DenseMatrix((1.1,1.2,1.3),(2.1,2.2,2.3))

  }

  test("test distribution"){
    val gaussian = Gaussian(0.0, 1.0)
    val g_samples = gaussian.sample(100)
    println(mean(g_samples))
    println(meanAndVariance(g_samples))
    println(stddev(g_samples))
    val poi = Poisson(3.0)
    val p_sample  = poi.sample(50)
    println(p_sample)

    val gau_matrix = new DenseMatrix(100, 10, gaussian.sample(1000).toArray)
    println(gau_matrix)
  }


}
