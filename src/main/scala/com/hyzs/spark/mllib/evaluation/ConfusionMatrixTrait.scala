package com.hyzs.spark.mllib.evaluation

/**
  * Created by xk on 2018/10/25.
  */
trait ConfusionMatrixTrait {

  /**
    *  condition positive number
    */
  def condition_positive: Int

  /**
    *  condition negative number
    */
  def condition_negative: Int

  def true_positive: Int

  def true_negative: Int

  def false_positive: Int

  def false_negative: Int

  /**
    * tpr = tp / p
    */
  def recall: Double

  /**
    * ppv = tp / (tp + fp)
    */
  def precision: Double

  /**
    * (tp + tn) / (p + n)
    */
  def accuracy: Double

  /**
    * f1 = 2 * ( ppv * tpr) / (ppv + tpr)
    */
  def f1_score: Double

}