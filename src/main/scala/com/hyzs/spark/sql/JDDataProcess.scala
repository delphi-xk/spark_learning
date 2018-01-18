package com.hyzs.spark.sql

/**
  * Created by XIANGKUN on 2018/1/9.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel

object JDDataProcess {
  val conf = new SparkConf().setAppName("DataProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val hdConf = sc.hadoopConfiguration
  val fs = FileSystem.get(hdConf)
  val partitionNums = sqlContext.getConf("spark.sql.shuffle.partitions").toInt
  //  val allConf = conf.getAll
//  val hiveDir = allConf.filter(param => param._1 == "hive.metastore.warehouse.dir")
  val warehouseDir = "/hyzs/warehouse/hyzs.db/"
  //val hiveWareDir = "/user/hive/warehouse/"

  import sqlContext.implicits._
  //sqlContext.setConf("spark.sql.shuffle.partitions", "2048")
  val key = "user_id"

  def checkHDFileExist(filePath: String): Boolean = {
    val path = new Path(filePath)
    fs.exists(path)
  }

  def dropHDFiles(filePath: String): Unit = {
    val path = new Path(filePath)
    fs.delete(path, true)
  }

  def saveTable(df: DataFrame, tableName:String): Unit = {
    sqlContext.sql(s"drop table if exists hyzs.$tableName")
    val path = s"$warehouseDir$tableName"
    if(checkHDFileExist(path))dropHDFiles(path)
    println("xkqyj going to save")
    df.write
      .option("path",path)
      .saveAsTable(s"hyzs.$tableName")
  }
  
  def createDFfromCsv(path: String, delimiter: String = "\\t"): DataFrame = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter))
      .filter(row => row.length <= cols.length)
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  // filter malformed data
  def createDFfromRawCsv(header: Array[String], path: String, delimiter: String = ","): DataFrame = {
    val data = sc.textFile(path, partitionNums)
    val cols = header.map( col => StructField(col, StringType))
    val rows = data.map( lines => lines.split(delimiter))
        .filter(row => row.length <= cols.length)
        .map(fields => Row(fields: _*))
      val struct = StructType(cols)
      sqlContext.createDataFrame(rows, struct)
  }

  def createDFfromSeparateFile(headerPath: String, dataPath: String,
                               headerSplitter: String=",", dataSplitter: String="\\t"): DataFrame = {
    //println(s"header path: ${headerPath}, data path: ${dataPath}")
    val header = sc.textFile(headerPath)
    val fields = header.first().split(headerSplitter)
    createDFfromRawCsv(fields, dataPath, dataSplitter)
  }

  def processHis(df: DataFrame): DataFrame = {
    df.groupBy(key)
      .agg(count(key).as("count_id"), avg("user_payable_pay_amount").as("avg_pay_amount"))
      .selectExpr(key, "cast (count_id as string) count_id", "cast (avg_pay_amount as string) avg_pay_amount")
  }

  // ensure countCols can be counted(cast double)
  def labelGenerateProcess(taskName:String, countCols: Array[String], weight: Array[Double]): DataFrame = {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.Matrices

    if( countCols.length == weight.length){
      val allData = sqlContext.sql("select * from hyzs.all_data")
      val selectCols = countCols.map( col => s"cast ($col as double) $col")
      val label_data = allData
        .na.replace(countCols.toSeq, Map("\\N" -> "0.01"))
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
        .select("user_id", "label")
      userLabel
    } else {
      throw new Exception("cols and weight length should be equal!")
    }
  }

  // import pin7labels.txt, generate label and data for class training
  def generateClassLabelAndData(labelFilePath: String): Unit = {
    val allLabels = createDFfromRawCsv(Array("phone", "label", "user_id"), labelFilePath, "\\t")
    saveTable(allLabels, "pin_all_labels")
    val tasks = Array("c1", "c2", "c3", "c4", "c5", "c6", "c7")
    for(index <- 1 to 7){
      val classLabel = allLabels.select(
        $"user_id",
        when($"label" === s"$index","1").otherwise("0").as("label")
      )
      saveTable(classLabel, s"${tasks(index-1)}_label")
    }
    val classData = sqlContext.sql("select b.* from hyzs.pin_all_labels a, hyzs.all_data b where a.user_id = b.user_id ")
    saveTable(classData, "class_data")

  }

  // ensure dataFrame.columns contains filterCols
  def dataGenerateProcess(dataFrame: DataFrame, filterCols:Array[String]): DataFrame = {
    val newCols = dataFrame.columns diff filterCols
    dataFrame.selectExpr(newCols: _*)
  }

  def processEmpty(df: DataFrame): DataFrame = {
    df.na.fill("\\N")
      .na.replace("*", Map("" -> "\\N","null" -> "\\N", "NULL" -> "\\N"))
   //   .dropDuplicates(Seq(key))
  }

  def processNull(df: DataFrame): DataFrame = {
    df.na.fill("")
      .na.replace("*", Map("null" -> "", "NULL" -> "", "-9999" -> ""))
  }

  val testTables = List(
    //"dmr_rec_s_user_order_amount_one_month",
    "dmr_rec_s_user_order_amount_one_month_new",
    "dmr_rec_v_dmt_upf_s_d_0000017",
    "dmr_rec_v_dmt_upf_s_d_0000030",
    "dmr_rec_v_dmt_upf_s_d_0000034",
    "dmr_rec_v_dmt_upf_s_d_0000035",
    "dmr_rec_v_dmt_upf_s_d_0000056",

    "dmr_rec_v_dmt_upf_s_d_1",
    "dmr_rec_v_dmt_upf_s_d_10",
    "dmr_rec_v_dmt_upf_s_d_2",
    "dmr_rec_v_dmt_upf_s_d_21",
    "dmr_rec_v_dmt_upf_s_d_3",
    "dmr_rec_v_dmt_upf_s_d_34",

    "dmr_rec_v_dmt_upf_s_d_4",
    "dmr_rec_v_dmt_upf_s_d_42",
    "dmr_rec_v_dmt_upf_s_d_44",
    "dmr_rec_v_dmt_upf_s_d_45",
    "dmr_rec_v_dmt_upf_s_d_47",
    "dmr_rec_v_dmt_upf_s_d_48",

    "dmr_rec_v_dmt_upf_s_d_5",
    "dmr_rec_v_dmt_upf_s_d_50",
    "dmr_rec_v_dmt_upf_s_d_51",
    "dmr_rec_v_dmt_upf_s_d_52",
    "dmr_rec_v_dmt_upf_s_d_53",
    "dmr_rec_v_dmt_upf_s_d_55",
    "dmr_rec_v_dmt_upf_s_d_8",
    "dmr_rec_v_dmt_upf_s_d_9",
    "dmr_rec_v_dmt_upf_s_m_56"
  )

  def forTest(): DataFrame = {
    var joinedData = sqlContext.sql(s"select * from hyzs.${testTables(0)}")
    for(tableName <- testTables.drop(1)){
      val table = sqlContext.sql(s"select * from hyzs.$tableName")
      joinedData = joinedData.join(table, Seq(key), "left_outer")
    }
    joinedData
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
      val dataTable = dataGenerateProcess(allData, params._1)
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
      val dataTable = dataGenerateProcess(allData, params._1)
      saveTable(dataTable, s"${task}_test")
    }

  }

  def labelTraining(): Unit = {
    generateClassLabelAndData("/hyzs/files/pin7labels.txt")

  }

  def main(args: Array[String]): Unit = {
    // import txt to DataFrame
    sqlContext.sql("create database IF NOT EXISTS hyzs ")

    val data = sc.getConf.get("spark.processJob.dataPath")
    val header = sc.getConf.get("spark.processJob.headerPath")
    val tableStr = sc.getConf.get("spark.processJob.fileNames")
    val sampleRatio = sc.getConf.get("spark.processJob.SampleRatio")
    //val dropFlag = sc.getConf.get("spark.processJob.DropFlag")
    //val actionStage = sc.getConf.get("spark.processJob.ActionStage")
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
      val hisTable = processNull(processHis(hisData))
      saveTable(hisTable, validTables(0))
    }

    if(args.length > 1 && args(1) == "import_info") {
      for(tableName <- validTables.drop(1)){
        val headerPath=s"$header$tableName.txt"
        val dataPath=s"$data$tableName.txt"
        val table = processNull(createDFfromSeparateFile(headerPath=headerPath, dataPath=dataPath))
        //   .repartition(col(key))
        saveTable(table, tableName)
      }
    }

    // big table join process
    var result = sqlContext.sql(s"select * from hyzs.${validTables(0)}")
      .sample(withReplacement=false, sampleRatio.toDouble)

    // .cache()
    // .repartition(col(key))
    //  .persist(StorageLevel.MEMORY_AND_DISK)
    for(tableName <- validTables.drop(1)) {
      val table = sqlContext.sql(s"select * from hyzs.$tableName")
      result = result.join(table, Seq(key), "left_outer")
        .repartition(partitionNums, col(key))
        .persist()
      println(s"xkqyj $tableName count num: ${result.count()}")
      //  .dropDuplicates(Seq(key))
    }
    // process NA values, save all_data table
    //val allData = processEmpty(result)
    saveTable(result, "all_data")
    // if only for prediction, not need to split data or generate label table
    if (args.length>1 && args(1) == "predict"){
      predictModelData(result)
    } else {
      trainModelData(result)
    }

  }

}
