package com.hyzs.spark.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Created by Administrator on 2018/2/6.
  */
object JsonUtil {

  // because of AbstractMethodException, remove scala object mapper
  // val mapper = new ObjectMapper() with ScalaObjectMapper
  val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }


/*  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json:String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }*/
}
