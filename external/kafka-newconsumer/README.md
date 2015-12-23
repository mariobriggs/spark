# Spark Streaming + Kafka with Kafka's new Consumer API

This is a clone of the [Spark-Streaming-Kakfka module](https://github.com/apache/spark/tree/master/external/kafka) with the 'DirectStream' implementation ported to use the Kafka v0.9.0 new Consumer API.
All other features of the DirectStream implementation remain the same as documented [here](https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md).  

Note that the Kafka new Consumer API is still 'beta quality' in Kafka v0.9.0. One reason for putting out an implementation with the new Consumer API
is support for SSL/Auth in the same.

### Some points about this implementation  
 - I took the liberty of only implementing the DirectStream & KafkaRDD part. The older Receiver based implementation is not ported  
 - The external API on KafkaUtils does not include the Deserializers in the API, you instead set them up in the KafkaParams map.
 
# Basic Usage  

To read from Kafka in a Spark Streaming job, use KafkaUtils.createDirectStream:  

```  
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",  //important
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",   //important
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false")
    
val ssc = new StreamingContext(new SparkConf, Seconds(60))

val topics = Set("sometopic", "anothertopic")

val stream = KafkaUtils.createDirectStream[String, String](
  ssc, kafkaParams, topics)

```  

To read from Kafka in a non-streaming Spark job, use KafkaUtils.createRDD:

```  
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",  //important
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",   //important
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false")
    
val ssc = new StreamingContext(new SparkConf, Seconds(60))

val offsetRanges = Array(
  OffsetRange("sometopic", 0, 110, 220),
  OffsetRange("sometopic", 1, 100, 313),
  OffsetRange("anothertopic", 0, 456, 789)
)

val rdd = KafkaUtils.createRDD[String, String](
  sc, kafkaParams, offsetRanges)
  
``` 

### Issues noted with Kafka 0.9.0 Consumer API
 - KafkaConsumer.poll(0) does not return any records when consumer offset is set to a position where data is available. Only poll(700) worked on my local Mac  
 - KafkaConsumer.position() seen to not return, when called very frequently at intervals below 200 ms
 - TopicPartition class is not Serializable, hence issues arise when serializing/deserialing DirectKafkaInputDStream. Opened issue in [Kafka JIRA](https://issues.apache.org/jira/browse/KAFKA-3029) and issued PR. Looks like it will get fixed :-) 
 
