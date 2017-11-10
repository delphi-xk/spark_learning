package com.hyzs.spark.utils

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConversions.propertiesAsScalaMap


/**
  * Created by Administrator on 2017/9/28.
  */
object PropertyUtils {

  val transType: Properties = PropertyUtils.getProperty("trans_type.properties")
  val db: Properties = PropertyUtils.getProperty("db.properties")
  val cluster: Properties= PropertyUtils.getProperty("cluster.properties")

  //val testPath = "d:/workspace/properties/"
  /*
  val hadoopIp: String = cluster("hadoop_server_ip")
  val hadoopPort: String = cluster("hadoop_server_port")
  val hdfsPath = s"hdfs://$hadoopIp:$hadoopPort/hyzs/properties"
  sc.addFile(hdfsPath,true)
  val sparkAddPath: String = SparkFiles.get("properties")
  val business: Properties= PropertyUtils.getOutsideProperty(sparkAddPath+"/summary.properties")
  val dynamic: Properties = PropertyUtils.getOutsideProperty(sparkAddPath+"/dynamic.properties")
*/
  // for test
  val business: Properties = PropertyUtils.getProperty("summary.properties")
  val dynamic: Properties = PropertyUtils.getProperty("dynamic.properties")



  def getProperty(path: String): Properties ={
    val property = new Properties()
    property.load(PropertyUtils.getClass.getClassLoader.getResourceAsStream(path))
    property
  }

  def getOutsideProperty(path: String): Properties ={
    val property = new Properties()
    property.load(new FileInputStream(path))
    property
  }


}
