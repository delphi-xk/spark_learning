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

  test("vector operations"){
    val v3 = DenseVector.rangeD(1,5)
    val v4 = DenseVector.rangeD(1.0, 10.0, 2.0)
    val m1 = new DenseMatrix(1,4, v4.toArray)
    val m2 = new DenseMatrix(4,1, v4.toArray)
    println(v3)
    println(v4)
    println(argmax(v3))
    println(v3.dot(v4))
    println(v3 *:* v4)
    // a.cols == b.length
    // println(v3 * v4)

    val testM = v3 * m1
    println(testM)
    println(testM * v4)
    val eigenM = eig(testM)
    println(eigenM.eigenvectors)
    println(eigenM.eigenvalues)
    println(testM * eigenM.eigenvectors(::,1))
    println(eigenM.eigenvalues(1) * eigenM.eigenvectors(::,1))

  }

  test("test matrix multiply"){
    val v1 = DenseVector(0.1, 0.2, 0.3)
    val v2 = DenseVector(4.0,5.0,6.0)
    //println(v1 *:* v2)

    val m1 = new DenseMatrix(3,1, Array(0.1, 0.2, 0.3))
    val m2 = new DenseMatrix(1,3, Array(4.0,5.0,6.0))
    val m3 = new DenseMatrix(3,3, Array(1.0, 2.0, 3.0,
      4.0, 5.0 , 6.0, 7.0,8.0,9.0))
    println(m3)
    println(m1)
    println(m3( ::, 1))
    println(m3( ::, 1) * 5.0)
    println( m3 * m1)
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

  }

  test("test matrix funcs"){
/*    val gaussian = Gaussian(0.0, 1.0)
    val gau_matrix = new DenseMatrix(4, 4, gaussian.sample(16).toArray)*/
    val m = DenseMatrix((0.8, 0.8), (0.4, 0.6) )
    val eigen = eig(m)
    println("m: ")
    println(m)
    println("eigenvalues: ")
    println(eigen.eigenvalues)
    println("eigenvectors:")
    println(eigen.eigenvectors)
    println("m * eigVector(m): ")
    println(m * eigen.eigenvectors)
    println("m * eigVector(0): ")
    println(m * eigen.eigenvectors(::, 0))
    println("eigVector(0) * eigenvalues(0) :")
    println(eigen.eigenvectors(::, 0) * eigen.eigenvalues(0))
    println("diag(eigValue) * eigVector(m)")
    println(eigen.eigenvectors * diag(eigen.eigenvalues) )
    //println("inverse: " + inv(gau_matrix))
    println(" m ^4")
    println(m * m * m * m * eigen.eigenvectors(:: , 0))
    println(m * m * m * m * eigen.eigenvectors(:: , 1))
  }


}
