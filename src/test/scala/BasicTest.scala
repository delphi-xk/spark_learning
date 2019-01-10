import scala.concurrent.Future

object BasicTest {


  import concurrent.ExecutionContext.Implicits.global
  def concurrencyTest(): Unit = {
    var i,j = 0
    (0 to 100000).foreach(_ => Future{i = i +1})
    (0 to 100000).foreach(_ => j = j+1)
    Thread.sleep(1000)
    println(s"i:$i, j:$j")
  }


  def main(args: Array[String]): Unit = {
    concurrencyTest()
  }

}
