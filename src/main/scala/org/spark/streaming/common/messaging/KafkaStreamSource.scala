package org.spark.streaming.common.messaging

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.spark.streaming.common.config.KafkaConfiguration.{kafkaBrokers, repartition, numPartition}

class KafkaStreamSource (ssc: StreamingContext, topics: String, zKProperties: Map[String, String]) {
  val brokers: String = kafkaBrokers
  val topicSet: Set[String] = topics.split(",").toSet
  val kafkaParams = Map("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
  val numberOfRecievers = 3

  def getStream(): DStream[(String, String)] = {
    val dStream: InputDStream[(String, String)] =
      getTopicAndPartionsWithOffSet(topicSet) match {
        case None => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
        case Some(topicAndPartitionOffset) => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, topicAndPartitionOffset,
          (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()))
      }
    if (repartition) dStream.repartition(numPartition) else dStream
  }

  private def getTopicAndPartionsWithOffSet(topics: Set[String]): Option[Map[TopicAndPartition, Long]] = {
    val topicAndPartitionWithOffset = ZookeeperManager.get(zKProperties).kafkaRecorder.getOffset(topicSet.toSeq)

    val offsetDistinctValues = topicAndPartitionWithOffset.values.toList.distinct
    if (topicAndPartitionWithOffset.isEmpty || offsetDistinctValues.length == 1 && offsetDistinctValues.head == 0) None
    else Option(topicAndPartitionWithOffset)
  }

}