package com.hyzs.spark.utils

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiangkun on 2018/6/28.
  *
  * extends function getOrCreate of org.apache.spark.sql.SQLContext
  */
object HiveContextUtil {

  private val activeContext: InheritableThreadLocal[SQLContext] =
    new InheritableThreadLocal[SQLContext]
  @transient private val instantiatedContext = new AtomicReference[SQLContext]()


  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    val ctx = activeContext.get()
    if (ctx != null && !ctx.sparkContext.isStopped) {
      return ctx
    }

    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        new HiveContext(sparkContext)
      } else {
        ctx
      }
    }
  }
}
