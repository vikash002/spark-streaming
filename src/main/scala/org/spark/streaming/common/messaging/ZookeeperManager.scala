package org.spark.streaming.common.messaging

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable

sealed class ZookeeperManager(params: Map[String, String]) {

  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"
  private var zKClient: ZkClient = null
  var kafkaRecorder: KafkaZookeeperRecorder = null

  def init(): ZookeeperManager = {
    val props = new Properties()
    params.foreach(param => props.put(param._1, param._2))
    props.setProperty(AUTO_OFFSET_COMMIT, "false")
    val consumerConfig = new ConsumerConfig(props)

    zKClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)
    kafkaRecorder = new KafkaZookeeperRecorder

    this
  }

  class KafkaZookeeperRecorder {
    private val groupId = params("groupId")

    def commitOffset(commit: Map[TopicAndPartition, Long]): Unit = {
      if(zKClient == null) throw new IllegalStateException("Zookeeper client is null")
      for((topicAndPart, offset) <- commit) {
        try {
          val topicDir = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
          val zkPath = s"${topicDir.consumerOffsetDir}/${topicAndPart.partition}"
          ZkUtils.updatePersistentPath(zKClient, zkPath, offset.toString)
        } catch {
          case e: Exception => ""
        }
      }
    }

    def getOffset(topics: Seq[String]): Map[TopicAndPartition, Long] = {
      val partitions = ZkUtils.getPartitionsForTopics(zKClient, topics)
      val topicAndPartitionOffsetMap = mutable.Map[TopicAndPartition, Long]()

      partitions.foreach(topicAndPart => {
        val currentTopicName = topicAndPart._1
        topicAndPart._2.foreach(partitionId => {
          val checkPoint = getOffset(currentTopicName, partitionId)
          val topicAndPartition = TopicAndPartition(currentTopicName, partitionId)
          topicAndPartitionOffsetMap.put(topicAndPartition, checkPoint)
        })
      })
      topicAndPartitionOffsetMap.toMap
    }

    def getOffset(topic: String, partition: Int):Long = {
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      val zkPath = s"${topicDirs.consumerOffsetDir}/${partition}"
      ZkUtils.readDataMaybeNull(zKClient, zkPath)._1 match {
        case None => 0L
        case Some(x) => x.toLong
      }
    }
  }
}

object ZookeeperManager {
  var zkManager: Map[String, ZookeeperManager] = null

  def get(properties: Map[String, String]) = {
    val groupId = properties.get("group.id").get
    if(zkManager == null) {
      zkManager = Map[String, ZookeeperManager]()
    }
    if(!zkManager.contains(groupId)) {
      zkManager += groupId -> new ZookeeperManager(properties).init()
    }
    zkManager.get(groupId).get
  }

  def updateOffsetInZk(rdd: RDD[(String, String)], zkProperties: Map[String, String]): Unit = {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    val zookeeperOffsets = rdd.mapPartitionsWithIndex((i, partition) => {
      val offsetRange: OffsetRange = offsetRanges(i)

      if(checkForConsistency(zkProperties, offsetRange)) {
        ZookeeperManager.get(zkProperties).kafkaRecorder.commitOffset(Map(TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset))
        List(offsetRange).iterator
      } else List().iterator
    }).collect().toList
    if(zookeeperOffsets.nonEmpty) {
      //TODO log
    }
  }

  private def checkForConsistency(poperties: Map[String, String], offsetRange: OffsetRange): Boolean = {
    offsetRange.untilOffset > offsetRange.fromOffset && {
      var currentOffset = ZookeeperManager.get(poperties).kafkaRecorder.getOffset(offsetRange.topic, offsetRange.partition)
      if(currentOffset == 0) currentOffset = offsetRange.fromOffset
      currentOffset == offsetRange.fromOffset
    }
  }
}
