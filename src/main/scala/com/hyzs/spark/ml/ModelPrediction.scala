package com.hyzs.spark.ml

import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by xk on 2018/10/26.
  */
object ModelPrediction {


  def randomForest(): Unit = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val trainPath = ""
    val validPath = ""
    val testPath = ""

    val trainingData = MLUtils.loadLibSVMFile(sc, trainPath)
    val validData = MLUtils.loadLibSVMFile(sc, validPath)
    val testData = MLUtils.loadLibSVMFile(sc, testPath)
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    val testTPR = labelAndPreds.filter(r => r._1 == 1 && r._2 == 1).count.toDouble /
      labelAndPreds.filter(r => r._1 == 1).count
    val testPPV = labelAndPreds.filter(r => r._1 == 1 && r._2 == 1).count.toDouble /
      labelAndPreds.filter(r => r._2 == 1).count

    println("Test Error = " + testErr)
    println("Test tpr, ppv =  " + testTPR + testPPV)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    //    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    //    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

  }

  def GBT(): Unit = {
    val libsvmPath = "/user/hyzs/libsvm/particles_train.libsvm"
    val data: Seq[RDD[LabeledPoint]] = MLUtils.loadLibSVMFile(sc, libsvmPath).randomSplit(Array(0.3, 0.3, 0.4))

    val trainingData = data.head
    val validData = data(1)
    val testData = data(2)

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(5) // Note: Use more iterations in practice. eg. 10, 20
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(5)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

    // without validation
    // val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    val model = new GradientBoostedTrees(boostingStrategy).runWithValidation(trainingData, validData)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
//    val sameModel = GradientBoostedTreesModel.load(sc,
//      "target/tmp/myGradientBoostingClassificationModel")
  }
}
