package org.spark.streaming.common

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.spark.streaming.common.sparkjob.config.TestConfiguration._
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.config.KafkaConfiguration.kafkaBrokers
import org.spark.streaming.common.config.ZookeeperConfiguration._

trait ConfigurationForTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val uniqueUserId = (Math.random() * 1000).toInt.toString;
    withConfiguration(Map[String, String](
      UserBookingEventKafkaTopic -> "userbooking",
      UserBookingEventPartitions -> "1",
      DriverEventKafkaTopic -> "driver",
      DriverEventPartition -> "1",
      KafkaBrokers -> "localhost:6001",
      ZKGroupId -> "spark-test",
      ZKHost -> "localhost:6000",
      UserEsIndex -> "user_test_1",
      DriverEsIndex -> "driver_test",
      DemandSupplyEsIndex -> "demand_test_1",
      EsNodes -> "ec2-52-56-201-201.eu-west-2.compute.amazonaws.com",
      EsPort -> "9200",
      UserIndexType -> "user",
      DriverIndexType -> "driver",
      DemandSupplyEsIndexType -> "demand"
    ))
  }
}
