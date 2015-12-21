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

package org.apache.spark.streaming.kafka

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.SparkException
import scala.collection.JavaConverters._
import org.apache.kafka.common._

/**
 * Convenience methods for interacting with a Kafka cluster.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
 */
private[spark]
class KafkaCluster(val kafkaParams: Map[String, String]) extends Serializable {
 
  def getPartitions(topics: Set[String]): Set[TopicPartition] = {
    getPartitionInfo(topics).map { pi =>
      new TopicPartition(pi.topic, pi.partition)
    }
  }

  def getPartitionsLeader(topics: Set[String]): Map[TopicPartition, String] = {
    getPartitionInfo(topics).map { pi =>
      new TopicPartition(pi.topic, pi.partition) -> pi.leader.host
    }.toMap
  }

  def getPartitionInfo(topics: Set[String]): Set[PartitionInfo] = {
    withConsumer { consumer =>
      topics.flatMap { topic =>
        Option(consumer.partitionsFor(topic)) match {
          case None =>
            throw new SparkException("Topic doesnt exist " + topic)
          case Some(piList) => piList.asScala.toList
        }
      }
    }.asInstanceOf[Set[PartitionInfo]]
  }


  def getLatestOffsets(
      topicPartitions: Set[TopicPartition]
    ): Map[TopicPartition, Long] =
    getOffsets(topicPartitions, OffsetResetStrategy.LATEST)


  def getEarliestOffsets(
      topicPartitions: Set[TopicPartition]
    ): Map[TopicPartition, Long] =
    getOffsets(topicPartitions, OffsetResetStrategy.EARLIEST)

  def getOffsets(
       topicPartitions: Set[TopicPartition],
       offsetResetType: OffsetResetStrategy
    ): Map[TopicPartition, Long] = {
    withConsumer{ consumer =>
      consumer.assign(topicPartitions.toList.asJava)
      offsetResetType match {
        case OffsetResetStrategy.EARLIEST => consumer.seekToBeginning(topicPartitions.toList: _*)
        case OffsetResetStrategy.LATEST => consumer.seekToEnd(topicPartitions.toList: _*)
        case _ => throw new SparkException("Unknown OffsetResetStrategy " + offsetResetType)
      }
      topicPartitions.map { tp =>
        val pos = consumer.position(tp)
        tp -> pos
      }.toMap
      
    }.asInstanceOf[Map[TopicPartition, Long]]
  }

  private def withConsumer
      (fn: KafkaConsumer[String, String] => Any): Any = {
    val consumer = new KafkaConsumer[String, String](
          kafkaParams.asInstanceOf[Map[String, Object]].asJava)
    try {
      fn(consumer)
    } finally {
      if (consumer != null) {
          consumer.close()
      }
    }
  }

}

