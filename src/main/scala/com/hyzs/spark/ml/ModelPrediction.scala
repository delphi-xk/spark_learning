package com.hyzs.spark.ml

import com.hyzs.spark.mllib.evaluation.ConfusionMatrix
import com.hyzs.spark.utils.SparkUtils._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini}
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by xk on 2018/10/26.
  */
object ModelPrediction {


  def prepareData(): (RDD[LabeledPoint],
    RDD[LabeledPoint],
    RDD[LabeledPoint]) = {
    val libsvmPath = "/user/hyzs/libsvm/particles_train.libsvm"
    val data: Seq[RDD[LabeledPoint]] = MLUtils.loadLibSVMFile(sc, libsvmPath).randomSplit(Array(0.3, 0.3, 0.4))
    (data(0), data(1), data(2))
  }

/*  def printEvaluation[T <: DecisionTreeModel](testData:RDD[LabeledPoint], model: T): Unit = {
    val predAndLabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }.collect()
    val confusion = new ConfusionMatrix(predAndLabels)

    println("model precision: " + confusion.precision)
    println("model recall: " + confusion.recall)
    println("model accuracy: " + confusion.accuracy)
  }*/

  def randomForest(trainingData: RDD[LabeledPoint],
                   validData: RDD[LabeledPoint],
                   testData:RDD[LabeledPoint]): Unit = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
    val startTime = System.currentTimeMillis()
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val costTime = System.currentTimeMillis() - startTime
    val predAndLabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }.collect()
    val confusion = new ConfusionMatrix(predAndLabels)

    println("cost time milliseconds: " + costTime)
    println("model precision: " + confusion.precision)
    println("model recall: " + confusion.recall)
    println("model accuracy: " + confusion.accuracy)
    println("model f1: " + confusion.f1_score)
  }

  def GBT(trainingData: RDD[LabeledPoint],
          validData: RDD[LabeledPoint],
          testData:RDD[LabeledPoint]): Unit = {

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(10) // Note: Use more iterations in practice. eg. 10, 20
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(8)
    // boostingStrategy.treeStrategy.setMaxBins(32)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    // boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    //boostingStrategy.treeStrategy.setImpurity(Entropy)
    //boostingStrategy.treeStrategy.setImpurity(Gini)

    // without validation
    // val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    val model = new GradientBoostedTrees(boostingStrategy).runWithValidation(trainingData, validData)

    val predAndLabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }.collect()
    val confusion = new ConfusionMatrix(predAndLabels)

    println("model precision: " + confusion.precision)
    println("model recall: " + confusion.recall)
    println("model accuracy: " + confusion.accuracy)
    println("model f1: " + confusion.f1_score)
  }


  def main(args: Array[String]): Unit = {
    val (trainingData, validData, testData) = prepareData()
    println("random forest =======")
    val randomForestModel = randomForest(trainingData, validData, testData)
    println("gbt =======")
    val gbt = GBT(trainingData, validData, testData)


  }
}
