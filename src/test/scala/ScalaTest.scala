//import scala.collection.JavaConversions._
import java.io._
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.hyzs.spark.utils.PropertyUtils
import org.apache.commons.lang.time.DateUtils
import org.junit.Test

import scala.annotation.tailrec
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math._
import scala.util.Random

/**
  * Created by Administrator on 2017/9/28.
  */
@Test
object ScalaTest {

  private var privateAge = 0

  def age : Int = privateAge

  def age_= (newValue: Int) : Unit = {
    if(newValue>privateAge) privateAge = newValue
  }

  val summary: Properties = PropertyUtils.getProperty("summary-bak.properties")

  def test(): Unit = {
/*    val business = PropertyUtils.business
    for ( (k,v) <- business){
      val props = v.replaceAll(" ","").split(",")
      val key = props(0)
      val slotDays = props(1)
      val date = props(2)
      val fields = props(3)
      val slotNum = props(4)
      println(s"table: $k, key:$key, slotDays:$slotDays")
    }*/
  }


  def test1() : Unit = {
    val cache = collection.mutable.Map[String, String]()
    val dstDb = PropertyUtils.db("default_database")
    val srcDb = PropertyUtils.db("source_database")
    println(dstDb,srcDb)

    for ( (k,v) <- PropertyUtils.transType){
      for ( e <- v.replaceAll("\\(|\\)","").split(",")){
        cache put (e,k)
      }
    }

    for ( (k,v) <- cache){
      println( s"key: $k, value: $v")
    }

    println(cache.keySet contains "8111")

/*    PropertyUtils.transType.foreach( ele => {
      println(s"${ele._1} => ${ele._2}")
    })*/
  }


  def test2():Unit = {
    val numList = List(1,2,3,4,5,6,7,8,9,10)

    // for loop execution with a yield
    val retVal = for{ a <- numList if a != 3; if a < 8 }yield a

    // Now print returned values using another loop.
    for( a <- retVal){
     // println( "Value of a: " + a )
    }


    for( i <- 0 until 10) {
      println(i)
    }
  }


  def test3() : Unit = {
    val filePath = "d:/workspace/types_test"
    //val typeMap = BusinessTest.returnTransMap()
    val typeList = returnTransList()
    val typeLength = typeList.size
    val random = Random
    val format = new java.text.SimpleDateFormat("yyyyMMdd")
    var date0 = format.parse("20170801")
    val bufferedWriter = new BufferedWriter(new FileWriter(new File(filePath)))

    for ( i <- 0 until 10000){
      val id = random.nextInt(10)
      val price = 1000*random.nextFloat()
      date0 = DateUtils.addHours(date0, random.nextInt(3))
      val create_date = format.format(date0)
      val tType = typeList(random.nextInt(typeLength-1))
      bufferedWriter.write(s"$id,$price,$create_date,$tType,测试_$i\n")
    }

    bufferedWriter.close()
  }


  def test4(): Unit = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val hisEndDate = "20171130"
    val slotNum = 10
    val slotSeconds = 86400*2
    val hisStartUnix = format.parse(hisEndDate).getTime - slotNum*slotSeconds*1000
    val hisStartDate = format.format(new Date(hisStartUnix))
    println(hisStartDate)

  }

  def test5(): Unit = {
    //var cols = new ListBuffer[String]
    var cols = new ArrayBuffer[String]
    cols += "count_id"
    cols += "sum_price"
    println(cols.mkString(","))
    val res = cols.map( x => s"${x}_i")
    println(res.mkString(","))
  }


  def fibonacci(n: Int): BigInt = {
    var a = BigInt(0)
    var b = BigInt(1)
    var idx = 0

    while (idx < n) {
      val tmp = a
      a = b
      b = tmp + a
      idx += 1
    }

    a
  }

  def fibonacci2(n: Int): BigInt = {
    @tailrec
    def loop(a: BigInt, b: BigInt, idx: Int = 0): BigInt =
      if (idx < n)
        loop(b, a + b, idx + 1)
      else
        a

    loop(0, 1)
  }


  def test6() : Unit = {
    val endDate = PropertyUtils.dynamic("start_date")
    val srcDb = PropertyUtils.db("source_database")
    val dstDb = PropertyUtils.db("default_database")
    for ((k,v) <- summary){
      val tableName = k
      val props = v.replaceAll(" ","").split(",")
      val key = props(0)
      val slotDays = props(1).toInt
      val dateFiled = props(2)
      val fields:Array[String] = props(3).split("#")
      val slotNum = props(4).toInt
      val slotSecs = 86400*slotDays
      val startDate = getHisStartDate(endDate, slotNum, slotSecs)
      val stampIndex = s"floor((UNIX_TIMESTAMP('$endDate','yyyyMMdd') - UNIX_TIMESTAMP(cast($dateFiled as string),'yyyyMMdd')) / $slotSecs)"

      val initColName = new ListBuffer[String]
      val selectBuilder = new StringBuilder()
      selectBuilder.append(s" count($key) as count_$key,")
      initColName += s"count_$key"
      for(field <- fields){
        selectBuilder.append(s" sum($field) as sum_$field,")
        selectBuilder.append(s" avg($field) as avg_$field,")
        initColName += s"sum_$field"
        initColName += s"avg_$field"
      }
      val selectString = selectBuilder.deleteCharAt(selectBuilder.length-1).toString()

      val hisSql = s"select $key, $selectString, " +
        s" $stampIndex as stamp  " +
        s" from $srcDb.$tableName " +
        s" where $dateFiled >= $startDate and $dateFiled <= $endDate "+
        s" group by $key, $stampIndex "
      println(hisSql)
    }
  }

  def getHisStartDate(endDate:String, slotNum:Int, slotSec:Long) : String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val hisStartUnix = format.parse(endDate).getTime - slotNum*slotSec*1000
    val hisStartDate = format.format(new Date(hisStartUnix))
    hisStartDate
  }


  def testSquare(num: Int): Boolean = {
    math.sqrt(num).toInt == math.sqrt(num)
  }

  def testSumSquare(num: Int): Boolean = {
    for( index <- 1 to math.sqrt(num).toInt){
      val i = num - math.pow(index,2).toInt
      if (i>0 && ScalaTest.testSquare(i)) {
        println(i, num-i)
        return true
      }
    }
    false
  }

  def testJdbcConnection(): Unit = {
    val driver = PropertyUtils.cluster.getProperty("hive_driver")

  }

  def swapAdjacent(buffer: ArrayBuffer[Int]): Unit = {
    for(i <- 0 until (buffer.length,2)){
      if(i < buffer.length-1){
        val tmp = buffer(i)
        buffer(i) = buffer(i+1)
        buffer(i+1) = tmp
      }
    }
  }


  def test8(): Unit = {
    val listBuffer = ArrayBuffer[Int]()
    for(i <- 0 until 7){
      listBuffer += Random.nextInt(100)
    }

    println(listBuffer)
    swapAdjacent(listBuffer)
    println(listBuffer)

  }

  def returnTransMap(): mutable.Map[String, String] = {
    val transType = mutable.Map[String, String]()
    for{
      (k,v) <- PropertyUtils.transType
      e <- v.replaceAll("\\(|\\)","").split(",")
    } transType put (e,k)
    transType
  }


  // transList   1000,1122, ...
  def returnTransList(): List[String] = {
    val list = mutable.ListBuffer[String]()
    for (e <- this.returnTransMap().keys){
      list += e
    }
    list.toList
  }

  def testZip(): Unit = {
    val list = Range(0,10,2)
    for((value, index) <- list.zipWithIndex){
      println(s"index: $index, value: $value ")
    }
  }


  def testList(): Unit = {
    val li = 1 :: 2 :: 3 :: Nil
    println(li.contains(2))
    println(li.exists( _ > 2))
    li.map( i => s"value_$i").foreach(println(_))
    println(li.mkString("|"))
  }

  def concatList(prefix:String, list1:Array[String], list2:Array[String]): Array[String] = {
    val res = new ArrayBuffer[String]()
    for(i <- list1){
      for(j <- list2){
        res += prefix+"_"+i+"_"+j
      }
    }
    res.toArray
  }

  def testSet(): Unit = {
    val stampRange = Range(0,5).map(x => x.toString).toArray
    val keys = PropertyUtils.transType.keySet().toArray(Array[String]("")).sortBy(x => x.drop(2).toInt)
    val test = concatList("res",keys, stampRange)
    test.foreach(println)
  }

  val endDate = PropertyUtils.dynamic("start_date")
  val slotSecs = 86400*30L
  val stampFunc: (String => String) = (oldDate: String) => {
    val format = new SimpleDateFormat("yyyyMMdd")
    val oldDateUnix = format.parse(oldDate).getTime / 1000L
    val endDateUnix = format.parse(endDate).getTime / 1000L
    val stamp = Math.floor((endDateUnix - oldDateUnix)/slotSecs)
    stamp.toInt.toString
  }



  def listToString(list: List[String]): String ={
    list match {
      case Nil => ""
      case str :: rest => str + " , "+ listToString(rest)
    }
  }

  def iterMap(): Unit = {
    val labelProcessMap = Map(
      "consume" ->
        (Array("jdmall_ordr_f0116", "jdmall_user_p0001"), Array(0.5, 0.5)),
      "value" ->
        (Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008"),
          Array(0.3, 0.3, 0.3, 0.1)),
      "risk" ->
        (Array("mem_vip_f0011", "mem_vip_f0001"), Array(0.5, 0.5))
    )
    for ((k,v) <- labelProcessMap){
      println(s"key:$k, value_1:${v._1.toString}, value_2:${v._2.toString}")
    }
    for( k <- labelProcessMap.keys){
      println(k)
    }
    val key = "consume"
    println(s"${labelProcessMap(key)}")
  }


  def testArgs(): Unit ={
    val args = Array("import1", "importXXX")
    if(args.length >0 && args(0) == "import1"){
      println(args(0))
    }
    if(args.length >1 && args(1) == "import2"){
      println(args(1))
    }
  }

  def getMd5(str: String): String = {
    MessageDigest.getInstance("MD5")
      .digest(str.getBytes)
      .map("%02X".format(_)).mkString
  }

  def testHash(): Unit ={
    val s1 = "jdjob1234568"
    val s2 = "jdjob1234567"
    val s3 = "向坤"
    val s4 = "asdfqw@!*&%*&#)(*!@&()#*!)!@<><?:"
    val s5 = "asdfqw@!*&%*&#)(*!@&()#*!)!@<><?:"
    println(s"s1: ${s1.hashCode} , ${s1.hashCode % 2500 % 15}")
    println(s"s2: ${s2.hashCode}, ${s2.hashCode % 2500 % 15}")
    println(s"s3: ${s3.hashCode}, ${s3.hashCode % 2500 % 15}")

    println(getMd5(s1))
    println(getMd5(s2))
    println(getMd5(s3))
    println(getMd5(s4))
    println(getMd5(s5))
  }

  def generateTestData: Unit ={
    val filePath = "d:/workspace/testdata"

    val bufferedWriter = new BufferedWriter(new FileWriter(new File(filePath)))


  }

  def main(args: Array[String]) {
    //println(returnTransMap)
    //test5()
    //for (i <- 0 to 20)println(s"fib($i): "+fibonacci(i))
    /*    println(testSumSquare(5))
        println(testSumSquare(8))
        println(testSumSquare(9))*/
    //test8()
    //test3()
    //testZip()
    //testList()
    //println(stampFunc("20180301"))
    //testSet
//    val li = List("aaa","bbb","ccc")
//    println(listToString(li))

    //iterMap()
    testHash


  }

}
