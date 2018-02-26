package com.hyzs.spark.bean

/**
  * Created by Administrator on 2018/2/7.
  */
abstract class BaseObj {
  def key:Int
  def value:String
}

case class Ob1(key:Int, value:String, fieldName:String) extends BaseObj
case class Ob2(key:Int, value:String, fieldName:String, map:Map[String,Int] ) extends BaseObj
case class StructInfo(index:Int, fieldName:String, fieldType:String)