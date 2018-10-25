package com.hyzs.spark.mllib.evaluation

/**
  * Created by xk on 2018/10/25.
  */
class ConfusionMatrix(data:Array[(Double, Double)]) extends ConfusionMatrixTrait {

  def total_numbers: Int = data.length
  /**
    * condition positive number
    */
  override def condition_positive: Int = data.count(_._2 == 1)

  /**
    * condition negative number
    */
  override def condition_negative: Int = data.count(_._2 == 0)

  override def true_positive: Int = data.count(x => x._1 == 1 && x._2 ==1)

  override def true_negative: Int = data.count(x => x._1 == 0 && x._2 ==0)

  override def false_positive: Int = data.count(x => x._1 ==1 && x._2 ==0)

  override def false_negative: Int = data.count(x => x._1 ==0 && x._2 ==1)

  /**
    * tpr = tp / p
    */
  override def recall: Double = true_positive.toDouble / condition_positive

  /**
    * ppv = tp / (tp + fp)
    */
  override def precision: Double = true_positive.toDouble / (true_positive + false_positive)

  /**
    * (tp + tn) / (p + n)
    */
  override def accuracy: Double = (true_positive + true_negative).toDouble / total_numbers

  /**
    * f1 = 2 * ( ppv * tpr) / (ppv + tpr)
    */
  def f1_score: Double = 2 * recall * precision / (recall + precision)
}