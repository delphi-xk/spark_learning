package com.hyzs.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
  * Created by xk on 2018/11/23.
  */
object DirectKafkaWordCount {
  def main(args: Array[String]) {

    val Array(brokers, groupId, topics) = Array("111.230.17.36:9094","testGroup01","kylin_streaming_topic")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("DirectKafkaWordCount")
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //messages.map(record => (record.key, record.value))

    // Get the lines, split them into words, count the words and print
/*    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()*/

    messages.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach( offsetRange => println(s"messages from [${offsetRange.toString()}]"))
      rdd.foreach{ item =>
        println(s"The record is ${item.toString}")
      }
    }



    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}