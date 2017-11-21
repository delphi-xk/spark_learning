package com.hyzs.spark.sql

/**
  * Created by Administrator on 2017/9/26.
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, SaveMode}
import com.hyzs.spark.utils.PropertyUtils
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.mutable

object BusinessTest {

  // for test
  //val hdfsPath = "hdfs://master:9000/hyzs/properties"


  val conf = new SparkConf().setAppName("BusinessTest")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._


  def main(args: Array[String]): Unit =  {
    sqlContext.setConf("spark.sql.shuffle.partitions", "53")
    //sqlContext.sql("set spark.sql.shuffle.partitions = 10")
    //generateSummary2()
    //testSave()
    processTypeTable(args(0))

  }

  def generateSummaryTables(): Unit = {
    val endDate = PropertyUtils.dynamic("start_date")
    val srcDb = PropertyUtils.db("source_database")
    val dstDb = PropertyUtils.db("default_database")

    for( (k,v) <- PropertyUtils.business){
      val tableName = k
      val props = v.replaceAll(" ","").split(",")
      val key = props(0)
      val slotDays = props(1).toInt
      val dateFiled = props(2)
      val fields:Array[String] = props(3).split("#")
      val slotNum = props(4).toInt
      val slotSecs = 86400*slotDays
      val startDate = getHisStartDate(endDate, slotNum, slotSecs)
      val stampIndex = s"floor((UNIX_TIMESTAMP('$endDate','yyyyMMdd') - " +
        s"UNIX_TIMESTAMP(cast($dateFiled as string),'yyyyMMdd')) / $slotSecs)"

      sqlContext.sql(s"drop table $dstDb.${tableName}_new")

      val initColName = new mutable.ListBuffer[String]
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
      val swp = sqlContext.sql(hisSql)

      var ids = sqlContext.sql(s"select $key from $srcDb.$tableName").distinct.orderBy(s"$key")

      for( index <- 0 until slotNum){
        val renameColName = initColName.map( name => col(name).as(s"${name}_$index"))
        val s = swp.filter(s"stamp=$index")
        val s_renamed = s.select(col(key) +:renameColName : _*)
        //val duplicate_cols = s_renamed.columns intersect ids.columns
        //remove duplicate columns
        //val id_cols = ids.columns.map(name => s"ids.$name")
        //val new_cols = s_renamed.columns.drop(0).map( name => s_renamed(name))
        //ids = ids.join(s_renamed, Seq(key), "left_outer")
        ids = ids.join(s_renamed, ids(key) === s_renamed(key), "left_outer")
        ids = ids.drop(s_renamed(key))
        //  .select(ids("*"),s_renamed("*"))
      }
      ids.write.format("orc").saveAsTable(s"$dstDb.${tableName}_new")
    }

  }


  def generateSummary2(): Unit = {
    val endDate = PropertyUtils.dynamic("start_date")
    val srcDb = PropertyUtils.db("source_database")
    val dstDb = PropertyUtils.db("default_database")

    for( (k,v) <- PropertyUtils.business){
      val tableName = k
      val props = v.replaceAll(" ","").split(",")
      val key = props(0)
      val slotDays = props(1).toInt
      val dateFiled = props(2)
      val fields:Array[String] = props(3).split("#")
      val slotNum = props(4).toInt
      val slotSecs = 86400*slotDays
      val startDate = getHisStartDate(endDate, slotNum, slotSecs)
      val stampIndex = s"""floor((UNIX_TIMESTAMP('$endDate','yyyyMMdd') -
        UNIX_TIMESTAMP(cast($dateFiled as string),'yyyyMMdd')) / $slotSecs)"""

     sqlContext.sql(s"drop table if exists $dstDb.${tableName}_new")

      val selectBuilder = new StringBuilder()
      selectBuilder.append(key+" ,")
      for(field <- fields){
        selectBuilder.append(field+" ,")
      }
      val selectString = selectBuilder.deleteCharAt(selectBuilder.length-1).toString()
      val sql = s""" select $selectString, $stampIndex as stamp
         from $srcDb.$tableName
         where $dateFiled >= $startDate and $dateFiled <= $endDate """
      println(sql)
      val swp = sqlContext.sql(sql)
      val aggCols = fields.flatMap( field => List(sum(field).as("sum_"+field), avg(field).as("avg_"+field) ))
      val tmpData = swp.groupBy(key, "stamp")
        .agg(count(key).as("count_"+key), aggCols :  _*)
      //tmpData.cache()

      var ids = tmpData.select(key).distinct //.orderBy(key)
      // drop cols : key, stamp
      val initCols = tmpData.columns.drop(2)

      for( index <- 0 until slotNum){
        val s = tmpData.filter(s"stamp=$index")
        val renameCols = initCols.map(name => col(name).as(s"${name}_$index"))
        val s_renamed = s.select(col(key) +:renameCols : _*)
        ids = ids.join(s_renamed, ids(key) === s_renamed(key), "left_outer")
        ids = ids.drop(s_renamed(key))
      }

      ids.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(s"$dstDb.${tableName}_new")
      //ids.write.format("orc").saveAsTable(s"$dstDb.${tableName}_new")
    }

  }

/*  def genCase(x: String) = {
    when($"t_types" <=> lit(x), $"sum_price").otherwise(0).alias("sum_price_"+x)
  }*/



/*  def genCase(t_type: String, filed: String) = {
    when($"t_types" <=> lit(t_type), col(s"sum_$filed"))
      .otherwise(0)
      .as(s"sum_${filed}_$t_type")
    when($"t_types" <=> lit(t_type), col(s"avg_$filed"))
      .otherwise(0)
      .as(s"avg_${filed}_$t_type")
  }*/

  def genCase(t_type: String) = {
    when($"t_types" <=> lit(t_type), col(s"sum_price"))
      .otherwise(0)
      .as(s"sum_price_$t_type")
    when($"t_types" <=> lit(t_type), col(s"avg_price"))
      .otherwise(0)
      .as(s"avg_price_$t_type")
  }


  //def genAgg(f: Column => Column)(x: String) = f(col(x)).alias(x)
  def genAgg(func: Column => Column)(field: String): Column = {
    func(col(field)).alias(s"${func}_${field}")
  }

  // rebuild summary table: client_no, fun1_col1_index1_index2, ...
  def processTypeTable(stampLength: String) : Unit = {
    val dateFiled = "create_date"
    val srcTable = "event_type_data"
    val countFields = Seq("price")
    val key = "client_no"
    val srcDb = PropertyUtils.db("source_database")
    val dstDb = PropertyUtils.db("default_database")
    val endDate = PropertyUtils.dynamic("start_date")
    val slotSecs = 86400*30L

    sqlContext.sql(s"drop table $dstDb.${srcTable}_new")
    val df = sqlContext.sql(s"select * from $srcDb.$srcTable where $dateFiled <= $endDate")

    val transMap = returnTransMap()
    val transFunc: (String => String) = (key: String) => {
      if(transMap.contains(key))
        transMap(key)
      else
        "others"
    }

    val stampFunc: (String => Int) = (oldDate: String) => {
      val format = new SimpleDateFormat("yyyyMMdd")
      val oldDateUnix = format.parse(oldDate).getTime / 1000L
      val endDateUnix = format.parse(endDate).getTime / 1000L
      val stamp = Math.floor((endDateUnix - oldDateUnix)/slotSecs)
      stamp.toInt
    }

    val stampRange = 0 until stampLength.toInt
    val typeRange = PropertyUtils.transType
      .keySet()
      .toArray(new Array[String](0))
      .sortBy(x => x.drop(2).toInt)

    val transUdf = udf(transFunc)
    val stampUdf = udf(stampFunc)

    val aggCols = countFields.flatMap(name => List(sum(name).as("sum_"+name), avg(name).as("avg_"+name)))

/*    df = df
      .groupBy(col(key),$"t_types")
      .agg(count(col(key)).as("count_"+key), aggCols : _*)
      .select($"client_no" +: typeRange.map(genCase): _*)*/

    val swp = df
      .withColumn("t_types", transUdf($"trans_type"))
      .withColumn("stamp", stampUdf(col(dateFiled)))
      .filter(s"stamp <= 5")
      .groupBy(col(key),$"t_types", $"stamp")
      .agg(count(col(key)).as("count_"+key), aggCols : _*)
      .repartition(col(key))
    swp.cache()
    println("swp cols: "+swp.columns.mkString(","))

    // drop cols client_no, t_types, stamp
    val cols = swp.columns.drop(3)

    // build table(small) join probe table(big)

    val ids = swp.select(key).distinct()
    //ids.cache()
    var result = ids

    for(s <- stampRange){
      var s_tmp = ids
      for (t <- typeRange){
        val t_tmp = swp.filter($"stamp" === s && $"t_types" === t)
          .select(col(key) +: cols.map(name => col(name).as(s"${name}_${s}_$t")): _*)
        s_tmp = t_tmp.join(s_tmp, Seq(key), "left_outer")
      }
      result = s_tmp.join(result, Seq(key), "left_outer").repartition(col(key))
    }

    result.write.mode(SaveMode.Overwrite).saveAsTable(s"$dstDb.${srcTable}_new")
  }



  def getHisStartDate(endDate:String, slotNum:Int, slotSec:Long) : String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val hisStartUnix = format.parse(endDate).getTime - slotNum*slotSec*1000L
    val hisStartDate = format.format(new Date(hisStartUnix))
    hisStartDate
  }

  // transMap   key:value   1000:ty1   1122:ty2 ...
  def returnTransMap(): mutable.Map[String, String] = {
    val transType = mutable.Map[String, String]()
    for ( (k,v) <- PropertyUtils.transType){
      for ( e <- v.replaceAll("\\(|\\)","").split(",")){
        transType put (e,k)
      }
    }
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



}
