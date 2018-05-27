package org.spark.streaming.common

import embeddedkafka.{EmbeddedKafkaConfig, EmbeddedKafka}
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.spark.streaming.common.sparkjob.config.Configuration._

import scala.collection.mutable.ListBuffer
import scala.concurrent.TimeoutException

trait EmbeddedKafkaBroker extends FunSuite with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
    createKafkaTopics()
  }

  override protected def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  protected def publishToKafka(topic: String, messages: String*): Unit =
    EmbeddedKafka.publishStringMessageToKafka(topic, messages:_*)

  protected def consumeFirstMessageFrom(topic: String): Option[String] = try
    Option(EmbeddedKafka.consumeFirstStringMessageFrom(topic))
  catch {
    case _: TimeoutException => None
  }

  protected def createTopic(name: String, numPartition: Int): Unit = {
    val zkClient = new ZkClient(s"127.0.0.1:${EmbeddedKafkaConfig.defaultConfig.zooKeeperPort}", 10000, 8000)
    AdminUtils.createTopic(zkClient, name, numPartition, 1)
  }

  private def createTopic(topicConfig: (String, Int)): Unit = {
    createTopic(topicConfig._1, topicConfig._2)
  }

  private def createKafkaTopics(): Unit = {
    Array(
      (userBookingtEventKafkaTopic, userBookingEventPartitions),
      (driverEventKafkaTopic, driverEventPartition)
    ).foreach(createTopic)
  }

  protected def consumeAllMessagesFrom(topic: String): List[String] = {
    val result: ListBuffer[String] = ListBuffer()
    var lastReadResult: Boolean = false
    do {
      consumeFirstMessageFrom(topic) match {
        case Some(message) =>
          result.append(message)
          lastReadResult = true
        case None =>
          lastReadResult = false
      }
    } while (lastReadResult)
    result.toList
  }
}
