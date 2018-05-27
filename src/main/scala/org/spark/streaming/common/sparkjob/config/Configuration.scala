package org.spark.streaming.common.sparkjob.config

import java.util.Properties

import org.apache.spark.SparkConf
import org.spark.streaming.common.config.{Config, GenericConfiguration}

import scala.collection.JavaConversions._

object Configuration {
  final val MaxCores = "spark.StreamingApp.max.cores"
  final val AppEnv = "spark.Streaming.env"
  final val AppName = "spark.StreamingApp.appName"
  final val StreamingInterval = "spark.Tasks.App.Interval"
  final val SparkMaster = "spark.StreamingApp.sparkMaster"
  final val ExecutorMemory = "spark.StreamingApp.executor.memory"
  final val UnPersist = "spark.StreamingApp.unpersist"
  final val UserEsIndex = "spark.Streaming.es.UserIndex"
  final val DriverEsIndex = "spark.Streaming.es.DriverIndex"
  final val DemandSupplyEsIndex = "spark.Streaming.es.DemandSupplyEsIndex"
  final val DemandSupplyEsIndexType = "spark.Streaming.es.DemandSupplyEsIndexType"
  final val EsNodes = "spark.StreamingApp.es.nodes"
  final val EsPort = "spark.StreamingApp.es.port"
  final val EsTimeout = "spark.StreamingApp.es.timeout"
  final val AutoCreate = "spark.StreamingApp.es.autocreate"
  final val MaxRatePerPartition = "spark.StreamingApp.kafka.maxRatePerPartition"
  final val UserBookingEventKafkaTopic = "userBooking.kafka.topic"
  final val UserBookingEventPartitions = "spark.StreamingApp.userBookingPartition"
  final val DriverEventKafkaTopic = "driverEvent.kafka.topic"
  final val DriverEventPartition = "spark.streamingApp.driverEventPartition"
  final val KafkaBatchSize = "spark.producer.batchSize"
  final val KafkaProducerClientId = "spark.producer.clientId"
  final val KafkaProducerAcks = "spark.producer.Acks"
  final val KafkaLingerMs = "spark.producer.LingerMs"
  final val KafkaBrokers = "spark.StreamingApp.kafkaBrokers"
  final val UserIndexType = "spark.Streaming.es.UserIndexType"
  final val DriverIndexType = "spark.Streaming.es.DriverIndexType"
  final val DemandSupplyAggInterval = "spark.streamingApp.demandSupply.agg.Interval"
  final val ModelPartitions = "spark.streamingApp.modelPartitions"

  private var propertyLoader: String => Option[String] = Config.getProperty

  private lazy val config = GenericConfiguration(Map(
       MaxCores -> propertyLoader(MaxCores),
      AppEnv -> propertyLoader(AppEnv),
      AppName -> propertyLoader(AppName),
      StreamingInterval -> propertyLoader(StreamingInterval),
      SparkMaster -> propertyLoader(SparkMaster),
      ExecutorMemory-> propertyLoader(ExecutorMemory),
      UnPersist -> propertyLoader(UnPersist),
      UserEsIndex -> propertyLoader(UserEsIndex),
      EsNodes -> propertyLoader(EsNodes),
      EsPort -> propertyLoader(EsPort),
      EsTimeout -> propertyLoader(EsTimeout),
      AutoCreate -> propertyLoader(AutoCreate),
      MaxRatePerPartition -> propertyLoader(MaxRatePerPartition),
      UserBookingEventKafkaTopic -> propertyLoader(UserBookingEventKafkaTopic),
      UserBookingEventPartitions -> propertyLoader(UserBookingEventPartitions),
      DriverEventKafkaTopic -> propertyLoader(DriverEventKafkaTopic),
      DriverEventPartition -> propertyLoader(DriverEventPartition),
      KafkaBatchSize -> propertyLoader(KafkaBatchSize),
      KafkaProducerClientId -> propertyLoader(KafkaProducerClientId),
      KafkaProducerAcks -> propertyLoader(KafkaProducerAcks),
      KafkaLingerMs -> propertyLoader(KafkaLingerMs),
      KafkaBrokers -> propertyLoader(KafkaBrokers),
      UserIndexType -> propertyLoader(UserIndexType),
      DriverIndexType -> propertyLoader(DriverIndexType),
      DriverEsIndex -> propertyLoader(DriverEsIndex),
      DemandSupplyAggInterval -> propertyLoader(DemandSupplyAggInterval),
      DemandSupplyEsIndexType -> propertyLoader(DemandSupplyEsIndexType),
      DemandSupplyEsIndex -> propertyLoader(DemandSupplyEsIndex),
      ModelPartitions -> propertyLoader(ModelPartitions)
  ))

  def appName = config.configValueAsString(AppName, "Spark Streaming App")

  def appEnv = config.configValue(AppEnv)

  def streamingInterval = config.configValueAsLong(StreamingInterval, 10)

  def sparkMaxCores = config.configValueAsString(MaxCores, "10")

  def sparkMaster = config.configValue(SparkMaster)

  def sparkExecutorMemory = config.configValueAsString(ExecutorMemory, "5g")

  def unPersist = config.configValueAsString(UnPersist, "false")

  def esNodes = config.configValue(EsNodes)

  def esPort = config.configValueAsString(EsPort, "9200")

  def esTimeout = config.configValueAsString(EsTimeout, "5m")

  def autoCreate = config.configValueAsString(AutoCreate, "true")

  def userEsIndex = config.configValue(UserEsIndex)

  def userEsIndexType = config.configValue(UserIndexType)

  def driverEsIndexType = config.configValue(DriverIndexType)

  def maxRatePerPartition = config.configValueAsString(MaxRatePerPartition, "1000")

  def userBookingtEventKafkaTopic = config.configValue(UserBookingEventKafkaTopic)

  def userBookingEventPartitions = config.configValueAsInt(UserBookingEventPartitions, 1)

  def driverEventKafkaTopic = config.configValue(DriverEventKafkaTopic)

  def driverEventPartition = config.configValueAsInt(DriverEventPartition, 1)

  def kafkaProducerClientId: String = config.configValueAsString(KafkaProducerClientId, "")

  def kafkaProducerAcks: Int = config.configValueAsInt(KafkaProducerAcks, 1)

  def kafkaBatchSize: Int = config.configValueAsInt(KafkaBatchSize, 16384)

  def kafkaLingerMs: Long = config.configValueAsLong(KafkaLingerMs, 500)

  def kafkaBrokers: String = config.configValue(KafkaBrokers)

  def driverEsIndex = config.configValue(DriverEsIndex)

  def demandSupplyEsIndex = config.configValue(DemandSupplyEsIndex)

  def demandSupplyEsIndexType = config.configValue(DemandSupplyEsIndexType)

  def demandSupplyAggInterval = config.configValueAsInt(DemandSupplyAggInterval, 20)

  def modelPartitions = config.configValueAsInt(ModelPartitions, 1)

  private[config] def overrideConfig(key: String, value: String) = {
    config.setConfig(key, value)
  }

  private[config] def setPropertyLoader(propertyLoader: (String) => Option[String]): Unit = {
    this.propertyLoader = propertyLoader
  }


  def kafkaProducerConfiguration: Properties = {
    val producerProperties = new Properties()
    producerProperties.putAll(Map(
      "client.id" -> kafkaProducerClientId,
      "bootstrap.servers" -> kafkaBrokers,
      "acks" -> kafkaProducerAcks.toString,
      "batch.size" -> kafkaBatchSize.toString,
      "linger.ms" -> kafkaLingerMs.toString,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    ))
    producerProperties
  }

  def sparkConfiguration = {
    new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.executorEnv.APP_ENV", appEnv)
      .set("spark.streaming.unpersist", unPersist)
      .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
      .set("spark.logConf", "true")
      .set("es.nodes", esNodes)
      .set("es.port", esPort)
      .set("es.http.timeout", esTimeout)
      .set("es.index.auto.create", autoCreate)
  }
}
