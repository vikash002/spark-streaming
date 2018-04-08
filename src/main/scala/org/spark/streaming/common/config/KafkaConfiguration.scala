package org.spark.streaming.common.config

object KafkaConfiguration {
  val KafkaBrokers = "spark.StreamingApp.kafkaBrokers"
  val Repartition = "spark.StreamingApp.repartition"
  val NumPartition = "spark.StreamingApp.numPartitions"

  val properyLoader: (String) => Option[String] = Config.getProperty
  val config = GenericConfiguration(Map(KafkaBrokers -> properyLoader(KafkaBrokers),
    Repartition -> properyLoader(Repartition),
    NumPartition -> properyLoader(NumPartition)))

  def kafkaBrokers:String = config.configValue(KafkaBrokers)
  def repartition: Boolean = config.configValueAsBoolean(Repartition, true)
  def numPartition: Int = config.configValueAsInt(NumPartition, 50)

}
