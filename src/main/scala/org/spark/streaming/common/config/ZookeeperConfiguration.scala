package org.spark.streaming.common.config

object ZookeeperConfiguration {
  val ZKGroupId = "spark.KAFKA_CONSUMER.groupId"
  val ZKHost = "spark.KAFKA_CONSUMER.host"
  val propertyLoader: String => Option[String] = Config.getProperty
  val config = GenericConfiguration(Map(ZKGroupId -> propertyLoader(ZKGroupId), ZKHost -> propertyLoader(ZKHost)))

  def zkGroupId = config.configValue(ZKGroupId)
  def zkHost = config.configValue(ZKHost)
}
