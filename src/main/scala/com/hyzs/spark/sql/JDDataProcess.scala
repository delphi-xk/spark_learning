package com.hyzs.spark.sql

/**
  * Created by XIANGKUN on 2018/1/9.
  */

import com.hyzs.spark.utils.SparkUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.mllib.linalg.{Matrices, Vector}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.hyzs.spark.utils.SparkUtils._

object JDDataProcess {

  val originalKey = "user_id"
  val key = "user_id_md5"
  import sqlContext.implicits._

  val preLabelMap = Map(
    "m1" ->
      (Array("jdmall_jdmuser_p0002816", "jdmall_user_p0001"), Array(0.5, 0.5)),
    "m2" ->
      (Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008"),
        Array(0.3, 0.3, 0.3, 0.1)),
    "m3" ->
      (Array("mem_vip_f0011", "mem_vip_f0001"), Array(0.5, 0.5))
  )

  def processHis(df: DataFrame): DataFrame = {
    df.groupBy(key, originalKey)
      .agg(count(key).as("count_id"),
        avg("before_prefr_amount").as("avg_prefr_amount"),
        avg("user_payable_pay_amount").as("avg_pay_amount"))
      .selectExpr(key, originalKey,
        "cast (count_id as string) count_id",
        "cast (avg_prefr_amount as string) avg_prefr_amount",
        "cast (avg_pay_amount as string) avg_pay_amount")
  }

  // ensure countCols can be counted(cast double)
  def labelGenerateProcess(key:String, allData:DataFrame, countCols: Array[String], weight: Array[Double]): DataFrame = {
    if( countCols.length == weight.length){
      val selectCols = countCols.map( col => s"cast ($col as double) $col")
      val label_data = processNull(allData.selectExpr(key +: selectCols : _*))
      // vector assembling
      val assembler = new VectorAssembler()
        .setInputCols(countCols)
        .setOutputCol("label_feature")
      val trans = assembler.transform(label_data)
      // scaling vectors
      val scaler = new MinMaxScaler()
        .setInputCol("label_feature")
        .setOutputCol("scaled_feature")
      val scaledModel = scaler.fit(trans)
      val scaledData = scaledModel.transform(trans)
      // calculate label values
      val weightMatrix = Matrices.dense(1, weight.length, weight)
      val multiplyFunc : (Vector => Double) = (scaled: Vector) => {
        weightMatrix.multiply(scaled).toArray.apply(0)
      }
      val multiplyUdf = udf(multiplyFunc)
      val userLabel = scaledData.withColumn("label", multiplyUdf(col("scaled_feature")))
        .select(key, "label")
      userLabel
    } else {
      throw new Exception("cols and weight length should be equal!")
    }
  }

  // ensure dataFrame.columns contains filterCols
  def dataGenerateProcess(dataFrame: DataFrame, filterCols:Array[String]): DataFrame = {
    val newCols = dataFrame.columns diff filterCols
    dataFrame.selectExpr(newCols: _*)
  }

  // generate label table and split data
  // no jdmall_ordr_f0116, change to jdmall_jdmuser_p0002816
  // no jdmall_user_f0007  jdmall_user_f0009 jdmall_user_f0014 mem_vip_f0008
  // no mem_vip_f0011
  /**
    * generate task_label, task_data tables
    * @param key
    * @param allData
    * @param labelProcessMap
    */
  def trainModelData(key:String, allData: DataFrame,
                     labelProcessMap:Map[String, (Array[String], Array[Double])]): Unit = {

    for( (task, params) <- labelProcessMap) {
      val labelTable = labelGenerateProcess(key, allData, params._1, params._2)
      saveTable(labelTable, s"${task}_label")
      val dataTable = dataGenerateProcess(allData, params._1)
      /*
      val splitData = dataTable.randomSplit(Array(0.3, 0.3, 0.4))
      val train = splitData(0)
      val valid = splitData(1)
      val test = dataTable
      saveTable(train, s"${task}_train")
      saveTable(valid, s"${task}_valid")
      saveTable(test, s"${task}_test")
      */
      saveTable(dataTable, s"${task}_data")

    }
  }

  // no label table
  def predictModelData(allData: DataFrame,
                       labelProcessMap:Map[String, (Array[String], Array[Double])]): Unit = {
    for((task, params) <- labelProcessMap){
      val dataTable = dataGenerateProcess(allData, params._1)
      saveTable(dataTable, s"${task}_test")
    }

  }

  def joinTableProcess(oldResult: DataFrame, tableName: String): DataFrame ={
    val newTable = sqlContext.sql(s"select * from hyzs.$tableName")
    val newResult = oldResult.join(newTable, Seq(key), "left_outer")
      //.dropDuplicates(Seq(key))
      //.repartition(partitionNums, col(key))
      //.persist(StorageLevel.MEMORY_AND_DISK)
    //newResult.first()
    println(s"xkqyj joined table: $tableName , ${newResult.rdd.partitions.size}")
    newResult
  }

  def importLabelTable(filePath:String):DataFrame = {
    val header = Array("phone", "label", key)
    createDFfromRawCsv(header, filePath, "\\t")
  }

  // generate multiple label table
  def multiLabelProcess(labelRange:Int, labelTable:DataFrame): Unit = {
    for(index <- 1 to labelRange){
      val label_i = labelTable.select(
        $"pin".as(key),
        when($"label" === s"$index","1").otherwise("0").as("label"))
      saveTable(label_i, s"label_$index")
    }

  }

  def main(args: Array[String]): Unit = {

    sqlContext.sql("create database IF NOT EXISTS hyzs ")

    val data = sc.getConf.get("spark.processJob.dataPath")
    val header = sc.getConf.get("spark.processJob.headerPath")
    val tableStr = sc.getConf.get("spark.processJob.fileNames")
    val sampleRatio = sc.getConf.get("spark.processJob.SampleRatio")

    val tables = tableStr.split(",")
    // check and filter if file exists
    val validTables = tables.filter(
      tableName => checkHDFileExist(s"$header$tableName.txt") && checkHDFileExist(s"$data$tableName.txt"))

    if(args.length >0 && args(0) == "import_business") {
      // process business table, result start with hisTable
      val tableName = validTables(0)
      val headerPath=s"$header$tableName.txt"
      val dataPath=s"$data$tableName.txt"
      val hisData = createDFfromSeparateFile(headerPath=headerPath, dataPath=dataPath)
      val hisTable = processHis(hisData)
        .repartition(numPartitions = partitionNums, col(key))
      saveTable(hisTable, validTables(0))
    }

    if(args.length > 1 && args(1) == "import_info") {
      for(tableName <- validTables.drop(1)){
        val headerPath=s"$header$tableName.txt"
        val dataPath=s"$data$tableName.txt"
        val table = createDFfromBadFile(headerPath=headerPath, dataPath=dataPath)
           .drop(originalKey)
           .repartition(numPartitions = partitionNums, col(key))
        saveTable(table, tableName)
      }
    }

    // big table join process
    var result = sqlContext.sql(s"select * from hyzs.${validTables(0)}")
     // .sample(withReplacement=false, sampleRatio.toDouble)
      .repartition(numPartitions = partitionNums, col(key))

    for(tableName <- validTables.drop(1)) {
      result = joinTableProcess(result, tableName)
    }

    result.repartition(numPartitions = partitionNums)
    saveTable(result, "all_data")
    // if only for prediction, not need to split data or generate label table
    if (args.length>1 && args(1) == "predict"){
      predictModelData(result, preLabelMap)
    } else {
      trainModelData(key, result, preLabelMap)
    }

  }

}
