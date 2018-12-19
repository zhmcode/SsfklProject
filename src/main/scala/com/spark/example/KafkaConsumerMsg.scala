package com.spark.example

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, streaming}

/**
  * Created by zhmcode on 2018/12/19 0019.
  */
object KafkaConsumerMsg {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumerMsg")
        val ssc = new StreamingContext(conf, streaming.Seconds(5))
        val topicParams = Map("test20" -> 1)
        val dstream = KafkaUtils.createStream(ssc, "192.168.121.31:2181,192.168.121.32:2181,192.168.121.33:2181", "testgroup_id", topicParams)
        dstream.map(_._2).count().print()
        ssc.start()
        ssc.awaitTermination()
    }

}
