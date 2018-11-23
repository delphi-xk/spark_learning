package com.hyzs.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
  * Created by xk on 2018/11/23.
  */
object DirectKafkaWordCount {
  def main(args: Array[String]) {

    val Array(brokers, groupId, topics) = Array("111.230.17.36:9094","testGroup02","jd_data01")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

/*    messages.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { item =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

        println(s"The record from topic [${o.topic}] is in partition ${o.partition} which offset from ${o.fromOffset} to ${o.untilOffset}")
        println(s"The record content is ${item.toList.mkString}")
      }
      rdd.count()
    }*/

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}