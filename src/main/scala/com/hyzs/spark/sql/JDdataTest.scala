package com.hyzs.spark.sql


import java.text.SimpleDateFormat


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
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
    val data = sqlContext.sql("select * from result_1201_2")
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



  }

}
