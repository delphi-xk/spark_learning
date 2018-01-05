package com.hyzs.spark.sql


import java.text.SimpleDateFormat

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
/**
  * Created by XIANGKUN on 2017/11/27.
  */
object JDdataTest {



  val delimiter = "\\t"
  val conf = new SparkConf().setAppName("DataTest")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sqlContext.setConf("spark.sql.shuffle.partitions", "337")
  sqlContext.setConf("spark.shuffle.file.buffer", "4096k")

  def createDFfromCsv(path: String, delimiter: String = "\\t"): DataFrame = {
    val data = sc.textFile(path)
    val header = data.first()
    val content = data.filter( line => line != header)
    val cols = header.split(delimiter).map( col => StructField(col, StringType))
    val rows = content.map( lines => lines.split(delimiter))
      .map(fields => Row(fields: _*))

    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  import org.apache.spark.sql._
  def createDFfromRawCsv(header: List[String], path: String, delimiter: String = ","): DataFrame = {
    val data = sc.textFile(path)
    val cols = header.map( col => StructField(col, StringType))
    val rows = data.map( lines => lines.split(delimiter))
      .map(fields => Row(fields: _*))
    val struct = StructType(cols)
    sqlContext.createDataFrame(rows, struct)
  }

  val hisTable = "dmr_rec_s_user_order_amount_one_month"
  val infoTables = List(

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
    "dmr_rec_v_dmt_upf_s_d_34"

//    "dmr_rec_v_dmt_upf_s_d_4",
//    "dmr_rec_v_dmt_upf_s_d_42",
//    "dmr_rec_v_dmt_upf_s_d_44",
//    "dmr_rec_v_dmt_upf_s_d_45",
//    "dmr_rec_v_dmt_upf_s_d_47",
//    "dmr_rec_v_dmt_upf_s_d_48",

//    "dmr_rec_v_dmt_upf_s_d_5",
//    "dmr_rec_v_dmt_upf_s_d_50",
//    "dmr_rec_v_dmt_upf_s_d_51",
//    "dmr_rec_v_dmt_upf_s_d_52",
//    "dmr_rec_v_dmt_upf_s_d_53",
//    "dmr_rec_v_dmt_upf_s_d_55",
//    "dmr_rec_v_dmt_upf_s_d_8",
//    "dmr_rec_v_dmt_upf_s_d_9",
//    "dmr_rec_v_dmt_upf_s_m_56"
  )


  def processHis():Unit = {
    import java.text.SimpleDateFormat
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql._
    import sqlContext.implicits._
    sqlContext.sql("use hyzs")
    val key = "user_id"
    val df1 = sqlContext.sql("select * from dmr_rec_s_user_order_amount_one_month")
    val startDate = "2017-10-21"
    val endDate = "2017-11-19"
    val slotSecs = 86400*7L

    val stampFunc: (String => Int) = (oldDate: String) => {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val oldDateUnix = format.parse(oldDate).getTime / 1000L
      val endDateUnix = format.parse(endDate).getTime / 1000L
      val stamp = Math.floor((endDateUnix - oldDateUnix)/slotSecs)
      stamp.toInt
    }

    val stampUdf = udf(stampFunc)
    val swp = df1
      .withColumn("stamp", stampUdf(col("dt")))
      .groupBy($"user_id",$"stamp").agg(count("user_id").as("count_id"), avg("user_payable_pay_amount").as("avg_pay_amount"))

    val ids = sqlContext.sql("select * from all_user_month")

    var result = ids
    val cols = Seq("count_id", "avg_pay_amount")
    for( index <- 0 to 4){
      val filtered = swp.filter(s"stamp = $index")
        .select(col(key) +:cols.map(name => col(name).as(s"${name}_stamp$index")) : _*)
      result = result.join(filtered, Seq(key), "left_outer")
    }
    sqlContext.sql("drop table order_amount_one_month_new")
    result.write.saveAsTable("order_amount_one_month_new")
  }

  def mergeStage():Unit = {
    val key = "user_id"
    val ids = sqlContext.sql("select * from all_user_month")

    var result_2 = ids
    for(table <- infoTables){
      val t1 = sqlContext.sql(s"select * from $table")
      val t2 = ids.join(t1, Seq("user_id"),"left_outer")
      result_2 = t2.join(result_2,Seq("user_id"),"left_outer")
    }
    //result_2.write.saveAsTable("result_1128_stage1")
    result_2.write.parquet("/hyzs/")
  }

  def mergeResult(): Unit = {
    val resultList = List (
      "order_amount_one_month_new",
      "result_1129_stage1",
      "result_1129_stage2",
      "result_1129_stage3",
      "result_1129_stage4"

    )

    sqlContext.sql("use hyzs")
    val df1 = sqlContext.sql("select * from order_amount_one_month_new")
    val df2 = sqlContext.sql("select * from result_1129_stage1 ")
    val df3 = sqlContext.sql("select * from result_1129_stage2 ")
    val res = df1.join(df2,Seq("user_id"),"left_outer").join(df3,Seq("user_id"), "left_outer")
    sqlContext.sql("drop table union_1130_1")
    res.write.saveAsTable("union_1130_1")




    sqlContext.sql("use hyzs")
    sqlContext.sql("drop table result_1130_2")
    val df4 = sqlContext.sql("select * from union_1130_1")
    val df5 = sqlContext.sql("select * from union_1129_2")
    df4.join(df5,Seq("user_id"),"left_outer").write.saveAsTable("result_1130_2")

  }



  def main(args: Array[String]): Unit = {
    for(table <- infoTables){
      createDFfromCsv(s"/hyzs/data/$table").write.saveAsTable(table)
    }

    //createDFfromCsv(s"/hyzs/data/dmr_rec_v_dmt_upf_s_d_3").write.saveAsTable("dmr_rec_v_dmt_upf_s_d_3")
    //createDFfromCsv(s"/hyzs/data/dmr_rec_v_dmt_upf_s_d_3").write.save("/hyzs/table/dmr_rec_v_dmt_upf_s_d_3")



    //val ids = df1.select("user_id").distinct()
    //ids.write.saveAsTable("all_user_month")

    //df1.groupBy().agg(max("dt"),min("dt"))

    /*    val stampFunc: (String => Int) = (oldDate: String) => {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val oldDateUnix = format.parse(oldDate).getTime / 1000L
      val startDateUnix = format.parse(startDate).getTime / 1000L
      val stamp = Math.floor((oldDateUnix - startDateUnix)/slotSecs)
      stamp.toInt
    }*/

  }

  // change column type double to string
  // and process NA values
  // split in 1207: val split = data.randomSplit(Array(0.7,0.29,0.01))
  def processNA(): Unit ={
    val df = sqlContext.sql("select * from hyzs.hive_result_1201")

    val cols = df.columns
    cols.filter(x => x.contains("stamp")).map(x => s"cast ($x as string) $x")
    val rest = cols.drop(11)
    val df2 = df.selectExpr("user_id"+:cols.filter(x => x.contains("stamp")).map(x => s"cast ($x as string) $x") ++: rest : _*)
    val result = df2.na.fill("\\N")
      .na.replace("*", Map("null" -> "\\N", "NULL" -> "\\N", "-9999" -> "\\N"))
      .dropDuplicates(Seq("user_id"))
    result.write.saveAsTable("hyzs.result_1204")

    val consume = result
      .drop("jdmall_ordr_f0116")
      .drop("jdmall_user_p0001")
    val splitData = consume.randomSplit(Array(0.7, 0.3))
    val train = splitData(0)
    val valid = splitData(1)
    train.write.saveAsTable("hyzs.result_consume_train_1204")
    valid.write.saveAsTable("hyzs.result_consume_valid_1204")



  }

  def consumeLabelProcess(): Unit ={
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.Matrices
    import org.apache.spark.ml.feature.StandardScaler

    val df = sqlContext.sql(" select * from result_1201_2")
      .na.replace("*", Map("\\N" -> "0.0"))
    val filterCols = List("jdmall_ordr_f0116", "jdmall_user_p0001")

    // trans data to double, then assemble to Vector
    val data = df.selectExpr("user_id", "cast ( jdmall_ordr_f0116 as double ) jdmall_ordr_f0116 ",
      " cast ( jdmall_user_p0001 as double ) jdmall_user_p0001 ")
    val assembler = new VectorAssembler()
      .setInputCols(Array("jdmall_ordr_f0116", "jdmall_user_p0001"))
      .setOutputCol("label_feature")
    val trans = assembler.transform(data)

    // normalize Vector features
/*    val scaler = new StandardScaler()
      .setInputCol("label_feature")
      .setOutputCol("scaled_feature")
      .setWithStd(true)
      .setWithMean(false)*/
    val scaler = new MinMaxScaler()
      .setInputCol("label_feature")
      .setOutputCol("scaled_feature")

    val scaledModel = scaler.fit(trans)
    val scaledData = scaledModel.transform(trans)
    scaledData.show

/*    val weight = Vectors.dense(0.4, 0.6)
    val value = Matrices.dense(1,2, Array(2,6))
    value.multiply(weight)   */

    val weightMatrix = Matrices.dense(1, 2, Array(0.5, 0.5))
    val multiplyFunc : (Vector => Double) = (scaled: Vector) => {
      weightMatrix.multiply(scaled).toArray.apply(0)
    }
    val multiplyUdf = udf(multiplyFunc)
    val userLabel = scaledData.withColumn("label", multiplyUdf(col("scaled_feature")))
      .select("user_id", "label")
    userLabel.show(false)
    userLabel.write.saveAsTable("hyzs.user_label_consume")

    val trainData = sqlContext.sql("select * from result_consume_train_1204")
    val validData = sqlContext.sql("select * from result_consume_valid_1204")
    val trainLabel = trainData.select("user_id").join(userLabel, Seq("user_id"), "left_outer")
    val validLabel = validData.select("user_id").join(userLabel, Seq("user_id"), "left_outer")
    trainLabel.write.saveAsTable("hyzs.user_label_consume_train")
    validLabel.write.saveAsTable("hyzs.user_label_consume_valid")


  }
  def valueLabelProcess(): Unit ={
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.Matrices

    val filterCols = List("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008")
    // mem_vip_f0008: 0/1  ->  0.01/1
    val df = sqlContext.sql(" select * from result_1205")
      .na.replace("*", Map("\\N" -> "0"))
      .na.replace("mem_vip_f0008", Map("0" -> "0.01"))

    val data = df.selectExpr("user_id", "cast ( jdmall_user_f0007 as double ) jdmall_user_f0007 ",
      "cast ( jdmall_user_f0009 as double ) jdmall_user_f0009",
      "cast ( jdmall_user_f0014 as double ) jdmall_user_f0014",
      "cast ( mem_vip_f0008 as double ) mem_vip_f0008")
    val assembler = new VectorAssembler()
      .setInputCols(Array("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014"))
      .setOutputCol("features")
    val trans = assembler.transform(data)
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaled_feature")
    val scaledModel = scaler.fit(trans)
    val scaledData = scaledModel.transform(trans)

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("scaled_feature","mem_vip_f0008"))
      .setOutputCol("label_features")
    val res = assembler2.transform(scaledData)
    //res.show

    val weightMatrix = Matrices.dense(1, 4, Array(0.3, 0.3, 0.3, 0.1))
    val multiplyFunc : (Vector => Double) = (scaled: Vector) => {
      weightMatrix.multiply(scaled).toArray.apply(0)
    }
    val multiplyUdf = udf(multiplyFunc)
    val userLabel = res.withColumn("label", multiplyUdf(col("label_features")))
      .select("user_id", "label")
    userLabel.show(false)
  }
  def splitValueData(): Unit = {
    val label = sqlContext.sql("select * from user_label_value")
    val splitData = label.randomSplit(Array(0.7, 0.3))
    val train_label = splitData(0)
    val valid_label = splitData(1)
    train_label.write.saveAsTable("hyzs.user_label_value_train")
    valid_label.write.saveAsTable("hyzs.user_label_value_valid")

//    val train_label = sqlContext.sql("select * from user_label_value_train")
//    val valid_label = sql("select * from user_label_value_valid")
    val data = sqlContext.sql("select * from result_1205")
    val trimCols = List("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008")
    val cols = data.columns.filter( col => !trimCols.contains(col) )
    val valueData = data.selectExpr(cols: _*)
    val train = train_label.select("user_id").join(valueData, Seq("user_id"), "left_outer")
    val valid = valid_label.select("user_id").join(valueData, Seq("user_id"), "left_outer")

    train.write.saveAsTable("hyzs.result_value_train_1205")
    valid.write.saveAsTable("hyzs.result_value_valid_1205")
  }

  def extractLabels(): Unit = {
    val data = sqlContext.sql("select * from user_label_value").select("label")
    val rdd = data.rdd.map(row => {
      val i = row.getDouble(0)
      f"$i%.1f"
    } )
    val df = rdd.toDF("label")
    val stats = df.groupBy("label")
      .agg(count("label").as("count_label"))
      .orderBy("label")

    stats.show(false)

  }

  def statusLabelFromPred(): Unit = {
    // create dataframe with column "value"
    val data = sqlContext.read.text("/hyzs/files/consume.pred")
    val labelFunc : (String => String) = (label) => {
      f"${label.toDouble}%.1f"
    }
    val labelUdf = udf(labelFunc)
    val processData = data.withColumn("pred_label", labelUdf(col("value")))

    val stats = processData.select("pred_label")
      .groupBy("pred_label")
      .agg(count("pred_label").as("count_label"))
      .orderBy("pred_label")


  }

  def riskLabelProcess(): Unit= {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.Matrices

    val filterCols = List("mem_vip_f0001", "mem_vip_f0011")
    val data = sqlContext.sql("select * from result_1205")
    var labelData = data
        .na.replace(Seq("mem_vip_f0011"), Map("\\N" -> "0"))
        .na.replace("mem_vip_f0001", Map("\\N" -> "0.01", "0" -> "0.01"))

    labelData = labelData.selectExpr("user_id",
          "cast (mem_vip_f0011 as double) mem_vip_f0011",
          "cast (mem_vip_f0001 as double) mem_vip_f0001")

    val assembler = new VectorAssembler()
      .setInputCols(Array("mem_vip_f0011"))
      .setOutputCol("features")
    val trans = assembler.transform(labelData)
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaled_feature")
    val scaledModel = scaler.fit(trans)
    val scaledData = scaledModel.transform(trans)

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("scaled_feature","mem_vip_f0001"))
      .setOutputCol("label_features")
    val res = assembler2.transform(scaledData)
    val weightMatrix = Matrices.dense(1, 2, Array(0.5, 0.5))
    val multiplyFunc : (Vector => Double) = (scaled: Vector) => {
      weightMatrix.multiply(scaled).toArray.apply(0)
    }
    val multiplyUdf = udf(multiplyFunc)
    val userLabel = res.withColumn("label", multiplyUdf(col("label_features")))
      .select("user_id", "label")

    userLabel.write.saveAsTable("hyzs.user_label_risk")
    userLabel.show(false)

  }

  def riskDataProcess(): Unit = {
    val userLabel = sqlContext.sql("select * from hyzs.user_label_risk")
    val filterCols = List("mem_vip_f0001", "mem_vip_f0011")
    val data = sqlContext.sql("select * from result_1205")
    val cols = data.columns diff filterCols
    val riskData = data.selectExpr(cols: _*)
    val split = riskData.randomSplit(Array(0.7, 0.29, 0.01))
    val train = split(0)
    val valid = split(1)
    val test = split(2)
    train.limit(10000000).write.saveAsTable("hyzs.result_risk_part_train")
    valid.limit(3000000).write.saveAsTable("hyzs.result_risk_part_valid")
    test.write.saveAsTable("hyzs.result_risk_test")

    val trainLabel = train.select("user_id")
        .join(userLabel, Seq("user_id"), "left_outer")
    val validLabel = valid.select("user_id")
      .join(userLabel, Seq("user_id"), "left_outer")
    trainLabel.write.saveAsTable("hyzs.user_label_risk_train")
    validLabel.write.saveAsTable("hyzs.user_label_risk_valid")

  }


  def importList(): Unit= {
    val data  = sc.textFile("/hyzs/files/pinlist.txt")
    data.map( pin => pin.toLowerCase)
      .toDF("user_id")
      .write.saveAsTable("jsbrpt_list")

  }

  def predProcess(): Unit ={
    //val pinList = sqlContext.sql("select * from jsbrpt_list")
    sqlContext.sql("use hyzs")
    sqlContext.sql("drop table result_risk_test_jsbrpt")
    val pinList = sqlContext.sql("select a.* from result_1205 a, jsbrpt_list b where a.user_id = b.user_id")
    val all = sqlContext.sql("select * from result_1205")

    val consumeFilterCols = List("jdmall_ordr_f0116", "jdmall_user_p0001")
    val consumeCols = all.columns diff consumeFilterCols
    val consumeTest = pinList.join(all, Seq("user_id"), "left_outer").selectExpr(consumeCols: _*)
    consumeTest.write.saveAsTable("hyzs.result_consume_test_jsbrpt")

    val valueFilterCols = List("jdmall_user_f0007", "jdmall_user_f0009", "jdmall_user_f0014", "mem_vip_f0008")
    val valueCols = all.columns diff valueFilterCols
    val valueTest = pinList.join(all, Seq("user_id"), "left_outer").selectExpr(valueCols: _*)
    valueTest.write.saveAsTable("hyzs.result_value_test_jsbrpt")

    //TODO: re-train risk model, and remove pay_syt_f0011
/*    val riskFilterCols = List("mem_vip_f0001", "mem_vip_f0011")
    val riskCols = all.columns diff riskFilterCols
    val riskTest = pinList.join(all, Seq("user_id"), "left_outer").selectExpr(riskCols: _*)
    riskTest.write.saveAsTable("hyzs.result_risk_test_jsbrpt")*/
    val riskFilterCols = List("mem_vip_f0001", "mem_vip_f0011", "pay_syt_f0011")
    val riskCols = pinList.columns diff riskFilterCols
    val riskTest = pinList.selectExpr(riskCols: _*)
    riskTest.write.saveAsTable("hyzs.result_risk_test_jsbrpt")
  }


  def generateLabelTables(): Unit = {
    val pin7 = sqlContext.sql("select * from pin7labels")
/*    val label1 = pin7.select(
      $"pin", $"label",
      when(pin7("label")==="1","1")
        .otherwise("0")
        .as("new_label_1")
    )
    pin7.groupBy("label").agg(count("label").as("count_label")).show
    */

/*    val pinAll = sqlContext.sql("select a.* from pin7labels a, result_1205 b where a.pin = b.user_id")
    pinAll.write.saveAsTable("hyzs.pin_all_labels")*/
    val pinAll = sqlContext.sql("select * from pin_all_labels")
    for( index <- 1 to 7){
      sqlContext.sql(s"drop table hyzs.pin_label_$index")
      pinAll.select(
        $"pin".as("user_id"),
        when($"label" === s"$index","1").otherwise("0").as("label"))
        .write.saveAsTable(s"hyzs.pin_label_$index")
    }

  }

  def generateLabelDataSets(): Unit = {
    val pinData = sqlContext.sql("select b.* from pin_all_labels a, result_1205 b where a.pin = b.user_id")
    pinData.write.saveAsTable("hyzs.pin_all_data")

    val split = pinData.randomSplit(Array(0.7, 0.2, 0.1))
    val train = split(0)
    val valid = split(1)
    val test = split(2)
    train.write.saveAsTable("hyzs.pin_data_train")
    valid.write.saveAsTable("hyzs.pin_data_valid")
    test.write.saveAsTable("hyzs.pin_data_test")

/*    sqlContext.sql("select b.label from pin_data_train a, pin_all_labels b where a.user_id = b.pin")
      .groupBy("label").agg(count("label").as("count_label")).show(false)*/

  }

  def processResultData(): Unit = {
    val labels = sqlContext.sql("select * from hyzs.pin7labels")
/*    val pin1 = createDFfromRawCsv(List("pin","pred_1"),"/hyzs/files/pin_1.result")
    val pin2 = createDFfromRawCsv(List("pin","pred_2"),"/hyzs/files/pin_2.result")
    val pin3 = createDFfromRawCsv(List("pin","pred_3"),"/hyzs/files/pin_3.result")
    val pin4 = createDFfromRawCsv(List("pin","pred_4"),"/hyzs/files/pin_4.result")
    val pin5 = createDFfromRawCsv(List("pin","pred_5"),"/hyzs/files/pin_5.result")
    val pin6 = createDFfromRawCsv(List("pin","pred_6"),"/hyzs/files/pin_6.result")
    val pin7 = createDFfromRawCsv(List("pin","pred_7"),"/hyzs/files/pin_7.result")

    val result = labels.join(pin1, Seq("pin"))
      .join(pin2, Seq("pin"))
      .join(pin3, Seq("pin"))
      .join(pin4, Seq("pin"))
      .join(pin5, Seq("pin"))
      .join(pin6, Seq("pin"))
      .join(pin7, Seq("pin"))
      .dropDuplicates(Seq("pin"))

    sqlContext.sql("drop table hyzs.pin_result")
    result.write.saveAsTable("hyzs.pin_result")*/

    val pin1 = createDFfromRawCsv(List("pin","pred_1"),"/hyzs/files/train_pin_1.result")
    val pin2 = createDFfromRawCsv(List("pin","pred_2"),"/hyzs/files/train_pin_2.result")
    val pin3 = createDFfromRawCsv(List("pin","pred_3"),"/hyzs/files/train_pin_3.result")
    val pin4 = createDFfromRawCsv(List("pin","pred_4"),"/hyzs/files/train_pin_4.result")
    val pin5 = createDFfromRawCsv(List("pin","pred_5"),"/hyzs/files/train_pin_5.result")
    val pin6 = createDFfromRawCsv(List("pin","pred_6"),"/hyzs/files/train_pin_6.result")
    val pin7 = createDFfromRawCsv(List("pin","pred_7"),"/hyzs/files/train_pin_7.result")

    val result = labels.join(pin1, Seq("pin"))
      .join(pin2, Seq("pin"))
      .join(pin3, Seq("pin"))
      .join(pin4, Seq("pin"))
      .join(pin5, Seq("pin"))
      .join(pin6, Seq("pin"))
      .join(pin7, Seq("pin"))
      .dropDuplicates(Seq("pin"))

    sqlContext.sql("drop table hyzs.train_pin_result")
    result.write.saveAsTable("hyzs.train_pin_result")

    val aggCols = (1 to 7).flatMap(
      index => List(
        max(col(s"pred_$index")),
        min(col(s"pred_$index")),
        avg(col(s"pred_$index"))
      )
    )
    // train pred aggregation data
    val row = result.select(aggCols: _*).rdd.first()

  }

  /**
    * v1 = max(train-yi), v2 = min(train-yi), v3 = avg(train-yi)
    * y' = 1 / (1 + exp(-5 * (y-v3)/(v1-v2)))
    *
    */

  def refinePredResult(): Unit = {

    /*    val v1 = Vectors.dense(Array(0.1, 0.2, 0.11))
    v1.toArray.max
    v1.toArray.min
    v1.toArray.sum / v1.toArray.length*/

     val raw = sqlContext.sql("select * from hyzs.pin_result")
      .selectExpr("pin","phone","label",
        "cast (pred_1 as double) pred_1",
        "cast (pred_2 as double) pred_2",
        "cast (pred_3 as double) pred_3",
        "cast (pred_4 as double) pred_4",
        "cast (pred_5 as double) pred_5",
        "cast (pred_6 as double) pred_6",
        "cast (pred_7 as double) pred_7"
      )
   val assembler = new VectorAssembler()
      .setInputCols(Array("pred_1", "pred_2","pred_3","pred_4","pred_5","pred_6","pred_7"))
      .setOutputCol("org_pred")
    val trans = assembler.transform(raw)
    /*
    val processFunc : (Vector => Vector) = (vector) => {
      val arr = vector.toArray
      val min = arr.min
      val max = arr.max
      val avg = arr.sum / arr.length
      val resArr = new Array[Double](arr.length)
      for( i <- arr.indices){
        resArr(i) = 1 / (1 + math.exp(-5 * (arr(i)-avg)/(max-min)) )
      }
      Vectors.dense(resArr)
    }
    */
    val trainModelAgg = List(
      List(0.482259,0.280304,0.38297577397471455),
      List(0.56917,0.282898,0.3283459518963921),
      List(0.493648,0.281042,0.3916670653715695),
      List(0.432489,0.294216,0.3357228556891765),
      List(0.445886,0.291254,0.3337333869873574),
      List(0.724011,0.296836,0.42110070212765977),
      List(0.492292,0.281987,0.29871587819919826)
    )

    val processFunc: (Vector => Vector) = (vector) => {
      val arr = vector.toArray
      val resArr = new Array[Double](arr.length)
      for(i <- arr.indices){
        val max = trainModelAgg(i)(0)
        val min = trainModelAgg(i)(1)
        val avg = trainModelAgg(i)(2)
        resArr(i) = 1 / (1 + math.exp(-5 * (arr(i)-avg)/(max-min)) )
      }
      Vectors.dense(resArr)
    }
    val processUdf = udf(processFunc)

    // label start with 1
    val findMaxIndexFunc: (Vector => Int) = (vector) => {
      val arr = vector.toArray
      arr.zipWithIndex.maxBy(_._1)._2 + 1
    }
    val findMaxUdf = udf(findMaxIndexFunc)

    // calculate accuracy
    /*val calAccFunc: ( Int,Int ) => Double = (label, pred_label) => {

    }*/

    sqlContext.sql("drop table hyzs.pin_result_refined")
    val refined = trans
      .withColumn("org_pred_label", findMaxUdf(col("org_pred")))
      .withColumn("refined_pred", processUdf(col("org_pred")))
      .withColumn("refined_pred_label", findMaxUdf(col("refined_pred")))
    refined.write.saveAsTable("hyzs.pin_result_refined")
    refined.select("phone","label",
      "org_pred","org_pred_label",
      "refined_pred","refined_pred_label")
      .rdd.coalesce(1).saveAsTextFile("/hyzs/files/pin_result_refined")

    val output = dissembleVector(refined, Seq("phone", "label"), "refined_pred", "pred")
    output
      .rdd
      .coalesce(1)
      .saveAsTextFile("/hyzs/pin_result_2")
  }



  /**
    * dissemble vector to columns with prefix colPrefix
    * @param df source dataframe
    * @param idCols reserved columns
    * @param vectorName vector column name
    * @param colPrefix generated new column prefix
    * @return
    */

  def dissembleVector(df: DataFrame, idCols: Seq[String], vectorName: String, colPrefix: String): DataFrame = {
    val vecSize = df.select(vectorName).first().getAs[Vector](0).size
    val newCols = (0 until vecSize).map( i => s"${colPrefix}_${i+1}")
    dissembleVectorWithSeq(df, idCols, vectorName, newCols)
  }

  /**
    * vector length should be the same with cols size
    * dissemble vector to columns
    * @param df source dataframe
    * @param idCols reserved columns
    * @param vectorName vector column name
    * @param cols new column names
    * @return
    */
  def dissembleVectorWithSeq(df: DataFrame, idCols: Seq[String], vectorName: String, cols: Seq[String]): DataFrame = {
    val vecToSeqFunc = udf((v: Vector) => v.toArray)
    val newCols = cols.zipWithIndex.map{
      case (col, index) => $"_tmp".getItem(index).as(col)
    }
    df.withColumn(
      "_tmp", vecToSeqFunc(col(vectorName)))
      .select( idCols.map(id => col(id)) ++: newCols: _* )
  }

}
