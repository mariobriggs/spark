/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.newapi._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectNewKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *   <groupId> is the name of kafka consumer group
 *   <auto.offset.reset> What to do when there is no initial offset in Kafka or
 *                       if the current offset does not exist any more on the server
 *                       earliest: automatically reset the offset to the earliest offset
 *                       latest: automatically reset the offset to the latest offset
 * Example:
 *    $ bin/run-example streaming.DirectNewKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2 my-consumer-group earliest
 *
 */
object DirectNewKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectNewKafkaWordCount <brokers> <topics> <groupId> \
                            |<auto.offset.reset>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <groupId> is the name of kafka consumer group (can be arbitrary)
                            |  <auto.offset.reset> What to do when there is no initial offset
                            |                      in Kafka or if the current offset does not exist
                            |                      any more on the server
                            |                      earliest: automatically reset the offset
                            |                                to the earliest offset
                            |                      latest: automatically reset the offset
                            |                              to the latest offset
        """.stripMargin)
      System.exit(1)
    }

    // StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics, groupId, offsetReset) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Direct Kafka Wordcount (New Consumer API)")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
