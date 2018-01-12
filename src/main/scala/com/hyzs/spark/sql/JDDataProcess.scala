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

object JDDataProcess {


  val conf = new SparkConf().setAppName("DataProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._
  //sqlContext.setConf("spark.sql.shuffle.partitions", "2048")
  val key = "user_id"
  //val log = LogManager.getRootLogger

  def checkHDFileExist(filePath: String): Boolean = {
    val hdConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hdConf)
    val path = new Path(filePath)
    fs.exists(path)
  }

  def createDFfromCsv(path: String, delimiter: String = "\\t"): DataFrame = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter))
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct).repartition(col(key))
  }

  def createDFfromRawCsv(header: Array[String], path: String, delimiter: String = ","): DataFrame = {
    val data = sc.textFile(path)
    val cols = header.map( col => StructField(col, StringType))
    if(cols.length == data.first().split(delimiter).length){
      val rows = data.map( lines => lines.split(delimiter))
        .map(fields => Row(fields: _*))
      val struct = StructType(cols)
      sqlContext.createDataFrame(rows, struct).repartition(col(key))
    } else {
      throw new Exception(s"$path data columns not equal!")
    }
  }

  def createDFfromSeparateFile(headerPath: String, dataPath: String,
                               headerSplitter: String=",", dataSplitter: String="\\t"): DataFrame = {
    println(s"header path: ${headerPath}, data path: ${dataPath}")

    val header = sc.textFile(headerPath)
    val fields = header.first().split(headerSplitter)
    createDFfromRawCsv(fields, dataPath, dataSplitter)
  }

  def processHis(hisTable: String): DataFrame = {
    val df1 = sqlContext.sql(s"select * from hyzs.$hisTable")
    val swp = df1
      .groupBy(key).agg(count(key).as("count_id"), avg("user_payable_pay_amount").as("avg_pay_amount"))
      .selectExpr(key, "cast (count_id as string) count_id", "cast (avg_pay_amount as string) avg_pay_amount")
    sqlContext.sql(s"drop table hyzs.${hisTable}_new")
    swp.write.saveAsTable(s"hyzs.${hisTable}_new")
    swp
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
    //  sqlContext.sql(s"drop table hyzs.${taskName}_label")
    //  userLabel.write.saveAsTable(s"hyzs.${taskName}_label")
    } else {
      throw new Exception("cols and weight length should be equal!")
    }
  }

  // import pin7labels.txt, generate label and data for class training
  def generateClassLabelAndData(labelFilePath: String): Unit = {
    val allLabels = createDFfromCsv(labelFilePath)
    allLabels.write.saveAsTable("hyzs.pin_all_labels")
    val tasks = Array("c1", "c2", "c3", "c4", "c5", "c6", "c7")
    for(index <- 1 to 7){
      sqlContext.sql(s"drop table hyzs.${tasks(index-1)}_label")
      val classLabel = allLabels.select(
        $"pin".as("user_id"),
        when($"label" === s"$index","1").otherwise("0").as("label")
      )
      classLabel.write.saveAsTable(s"hyzs.${tasks(index-1)}_label")
    }
    sqlContext.sql("drop table hyzs.class_data")
    val classData = sqlContext.sql("select b.* from hyzs.pin_all_labels a, hyzs.all_data b where a.pin = b.user_id ")
    classData.write.saveAsTable("hyzs.class_data")

  }

  // ensure dataFrame.columns contains filterCols
  def dataGenerateProcess(dataFrame: DataFrame, filterCols:Array[String]): DataFrame = {
    val newCols = dataFrame.columns diff filterCols
    dataFrame.selectExpr(newCols: _*)
  }

  def processNA(df: DataFrame): DataFrame = {
    df.na.fill("\\N")
      .na.replace("*", Map("null" -> "\\N", "NULL" -> "\\N", "-9999" -> "\\N"))
   //   .dropDuplicates(Seq(key))
  }

  def forTest(): DataFrame = {
    val tables = List(
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
    var joinedData = sqlContext.sql(s"select * from hyzs.${tables(0)}")
    for(tableName <- tables.drop(1)){
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
      sqlContext.sql(s"drop table hyzs.${task}_label")
      labelTable.write.saveAsTable(s"hyzs.${task}_label")
      val dataTable = dataGenerateProcess(allData, params._1)
      val splitData = dataTable.randomSplit(Array(0.7, 0.2, 0.1))
      val train = splitData(0)
      val valid = splitData(1)
      val test = splitData(2)
      sqlContext.sql(s"drop table hyzs.${task}_train")
      sqlContext.sql(s"drop table hyzs.${task}_valid")
      sqlContext.sql(s"drop table hyzs.${task}_test")
      train.write.saveAsTable(s"hyzs.${task}_train")
      valid.write.saveAsTable(s"hyzs.${task}_valid")
      test.write.saveAsTable(s"hyzs.${task}_test")

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
      sqlContext.sql(s"drop table hyzs.${task}_test")
      dataTable.write.saveAsTable(s"hyzs.${task}_test")
    }

  }

  def main(args: Array[String]): Unit = {
    // import txt to DataFrame
    sqlContext.sql("create database IF NOT EXISTS hyzs ")

/*    if(args(0) == "test"){
      joinedData = forTest()
    }
*/
    val data = sc.getConf.get("spark.processJob.dataPath")
    val header = sc.getConf.get("spark.processJob.headerPath")
    val tableStr = sc.getConf.get("spark.processJob.fileNames")
    val tables = tableStr.split(",")
    val validTables = tables.filter(
      tableName => checkHDFileExist(s"$header$tableName.txt") && checkHDFileExist(s"$data$tableName.txt"))

    for(tableName <- validTables){
      val headerPath=s"$header$tableName.txt"
      val dataPath=s"$data$tableName.txt"
      val table = createDFfromSeparateFile(headerPath=headerPath, dataPath=dataPath)
      sqlContext.sql(s"drop table if exists hyzs.$tableName")
      table.write.saveAsTable(s"hyzs.$tableName")
    }

    // process business table, res start with hisTable
    var joinedData = processHis(validTables(0))
    // big table join process
    for(tableName <- validTables.drop(1)){
      val table = sqlContext.sql(s"select * from hyzs.$tableName")
      joinedData = joinedData.join(table, Seq(key), "left_outer")
    }

    sqlContext.sql("drop table hyzs.all_data")
    // process NA values, save all_data table
    val allData = processNA(joinedData)
    allData.write.saveAsTable("hyzs.all_data")

    // if only for prediction, not need to split data or generate label table
    if (args.length>0 && args(0) == "predict"){
      predictModelData(allData)
    } else {
      trainModelData(allData)
    }

  }


}
