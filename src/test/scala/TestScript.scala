import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


 def getIndexers(df: DataFrame, col: String): (String, StringIndexerModel) = {
    val indexer = new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexer")
      .setHandleInvalid("skip")
      .fit(df)
    (col, indexer)
 }

val df = sql("select * from test.test_data")
val test = df.select(df.columns.take(10).map(col): _*)
var res = test.na.fill("0.0").na.replace("*", Map( "null" -> "0.0", " "->"0.0" ))

val cols = res.columns.drop(1)
val indexerArray = cols.map(col => getIndexers(res, col))
val pipeline = new Pipeline().setStages(Array(indexerArray.map(_._2): _*))
val model = pipeline.fit(res)
val transformed = model.transform(res)

    val labeledFunc: (Vector => LabeledPoint) = (vector: Vector) =>{
      LabeledPoint(0.0, vector)
    }
    val labeledUdf = udf(labeledFunc)
    val res5 = res4.withColumn("labeled_point", labeledUdf(col("features"))).selectExpr("client_no", "features", "labeled_point")


transformed.columns.filter(str => str.matches("\\S+_indexer"))
val indexerCols = transformed.columns.filter(str => str.endsWith("_indexer"))

val assembler = new VectorAssembler().setInputCols(indexerCols).setOutputCol("features")

val model2 = new Pipeline().setStages(Array(assembler)).fit(transformed)

val res3 = model2.transform(transformed).selectExpr("client_no" +: indexerCols :+ "features": _*)

val labelData = res5.select("features").rdd.map{ x: Row => x.getAs[Vector](0)}.map(labeledFunc)
 MLUtils.saveAsLibSVMFile(labelData.coalesce(1), "/hyzs/data/test_libsvm")

 res5.first().map{ row => row.getSeq[Double]}


 val cols = (1 to 15).map(i => "col_"+i).map(col => StructField(col, StringType))
 val arrs = Array("t1\tt2\ta,,,b,cc,,,d,,,e,,,,", "T1\tT2\tA,,,B,CC,,,,,,E,,F")
 val rdd = sc.makeRDD(arr)
 val data = rdd.map( (row:String) => {
  val arr = row.split("\\t", -1)
  val res = arr(0) +: arr(1) +: arr(2).split(",", -1)
  res
}).map(fields => Row(fields: _*))
 val struct = StructType(cols)
 val table = sqlContext.createDataFrame(data, struct)


val cols = (1 to 245).map(i => "col_"+i).map(col => StructField(col, StringType))

val header = sc.textFile("/hyzs/test/fea.header").first().split(",").map(col => StructField(col, StringType))
val dataFile = sc.textFile("/hyzs/test/feature_data.txt")
val data = dataFile.map( (row:String) => { 
    val arr = row.split("\\t", -1)
    arr(0) +: arr(1) +: arr(2).split(",", -1)
  })
  .filter( arr => arr.length <= header.length)
  .map(fields => Row(fields: _*))
val struct = StructType(header)
val table = sqlContext.createDataFrame(rddData, struct)