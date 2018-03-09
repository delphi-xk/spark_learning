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
  import spark.implicits._

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
  def labelGenerateProcess(taskName:String, countCols: Array[String], weight: Array[Double]): DataFrame = {
    if( countCols.length == weight.length){
      val allData = spark.sql("select * from hyzs.all_data")
      val selectCols = countCols.map( col => s"cast ($col as double) $col")
      val label_data = processEmpty(allData, countCols)
        .selectExpr(key +: selectCols : _*)

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

  // process empty value for label generation
  def processEmpty(df: DataFrame, cols: Seq[String]): DataFrame = {
    df.na.fill("0.0")
      .na.replace(cols, Map("" -> "0.0","null" -> "0.0", "NULL" -> "0.0"))
   //   .dropDuplicates(Seq(key))
  }

  def processNull(df: DataFrame): DataFrame = {
    df.na.fill("0")
      .na.replace("*", Map("null" -> "0", "NULL" -> "0", "-9999" -> "0"))
  }

  // generate label table and split data
  def trainModelData(allData: DataFrame): Unit = {
    val labelProcessMap = Map(
      "m1" ->
        (Array("jdmall_ordr_f0116", "jdmall_user_p0001"), Array(0.5, 0.5)),
      "m2" ->
        (Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008"),
          Array(0.3, 0.3, 0.3, 0.1)),
      "m3" ->
        (Array("mem_vip_f0011", "mem_vip_f0001"), Array(0.5, 0.5))
    )
    for( (task, params) <- labelProcessMap) {
      val labelTable = labelGenerateProcess(task, params._1, params._2)
      saveTable(labelTable, s"${task}_label")
      val dataTable = dataGenerateProcess(allData, params._1 :+ originalKey)
      val splitData = dataTable.randomSplit(Array(0.7, 0.2, 0.1))
      val train = splitData(0)
      val valid = splitData(1)
      val test = splitData(2)
      saveTable(train, s"${task}_train")
      saveTable(valid, s"${task}_valid")
      saveTable(test, s"${task}_test")
    }
  }

  // no label table
  def predictModelData(allData: DataFrame): Unit = {
    val labelProcessMap = Map(
      "m1" ->
        (Array("jdmall_ordr_f0116", "jdmall_user_p0001"), Array(0.0, 0.0)),
      "m2" ->
        (Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008"),
          Array(0.0, 0.0, 0.0, 0.0)),
      "m3" ->
        (Array("mem_vip_f0011", "mem_vip_f0001"), Array(0.0, 0.0))
    )
    for((task, params) <- labelProcessMap){
      val dataTable = dataGenerateProcess(allData, params._1 :+ originalKey)
      saveTable(dataTable, s"${task}_test")
    }

  }

  def joinTableProcess(oldResult: DataFrame, tableName: String): DataFrame ={
    val newTable = spark.sql(s"select * from hyzs.$tableName")
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

    spark.sql("create database IF NOT EXISTS hyzs ")

    val data = conf.get("spark.processJob.dataPath")
    val header = conf.get("spark.processJob.headerPath")
    val tableStr = conf.get("spark.processJob.fileNames")
    val resTable = Option(conf.get("spark.processJob.resultTable")).getOrElse("all_data")

    val tables = tableStr.split(",")
    // check and filter if file exists
    val validTables = tables.filter(
      tableName => checkHDFileExist(s"$header$tableName.txt") && checkHDFileExist(s"$data$tableName.txt"))

    if(args.length >0 && (args(0) == "import_business"||args(0) == "import_all") ) {
      // process business table, result start with hisTable
      val tableName = validTables(0)
      val headerPath=s"$header$tableName.txt"
      val dataPath=s"$data$tableName.txt"
      val hisData = createDFfromSeparateFile(headerPath=headerPath, dataPath=dataPath)
      val hisTable = processHis(hisData)
        .repartition(numPartitions = partitionNums, col(key))
      saveTable(hisTable, validTables(0))
    }

    if(args.length >0 && (args(0) == "import_info")||args(0) == "import_all") {
      for(tableName <- validTables.drop(1)){
        val headerPath=s"$header$tableName.txt"
        val dataPath=s"$data$tableName.txt"
        val table = createDFfromBadFile(headerPath=headerPath, dataPath=dataPath)
           .drop(originalKey)
           .repartition(numPartitions = partitionNums, col(key))
        saveTable(table, tableName)
      }
    }

    var result: Option[Dataset[Row]] = None
    if (args.length>1 && args(1) == "skip_join") {
      result = Some(spark.table(s"hyzs.$resTable"))
    } else {
      // big table join process
      result = Some(spark.sql(s"select * from hyzs.${validTables(0)}")
        // .sample(withReplacement=false, sampleRatio.toDouble)
        .repartition(numPartitions = partitionNums, col(key)))

      for(tableName <- validTables.drop(1)) {
        result = Some(joinTableProcess(result.get, tableName))
      }
    }
    saveTable(result.get, "all_data")
    // if only for prediction, not need to split data or generate label table
    if (args.length>1 && args(1) == "predict"){
      predictModelData(result.get)
    } else {
      trainModelData(result.get)
    }

  }

}
