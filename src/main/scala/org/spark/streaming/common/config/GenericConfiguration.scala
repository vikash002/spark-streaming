package org.spark.streaming.common.config

class GenericConfiguration {

  private lazy val configMap = scala.collection.mutable.Map[String, String]()

  def configValue(key: String) = configMap.get(key) match {
    case Some(v) => v
    case None => throw new RuntimeException(s"Configuration property $key must be defined in Config Service")
  }

  def configValueAsBoolean(key: String, defaultValue: Boolean) = configMap.get(key) match {
    case Some(v) => v.toBoolean
    case None => defaultValue
  }

  def configValueAsInt(key: String, defaultValue: Int) = configMap.get(key) match {
    case Some(v) => v.toInt
    case None => defaultValue
  }

  def configValueAsString(key: String, defaultValue: String) = configMap.get(key) match {
    case Some(v) => v
    case None => defaultValue
  }

  def configValueAsLong(key: String, defaultValue: Long) = configMap.get(key) match {
    case Some(v) => v.toLong
    case None => defaultValue
  }

  def configValues(keys: String*): Map[String, String] = {
    val result = scala.collection.mutable.Map[String, String]()
    for (key <- keys) {
      result.put(key, configValue(key))
    }
    result.toMap
  }

  def configs(): Map[String, String] = {
    configMap.toMap
  }

  def setConfig(key: String, value: String) = {
    configMap.put(key, value)
  }
}

object GenericConfiguration {
  def apply(configs: Map[String, Option[String]]): GenericConfiguration = {
    val configuration: GenericConfiguration = new GenericConfiguration
    for (key <- configs.keys) {
      configs.get(key).get match {
        case Some(value) => configuration.setConfig(key, value)
        case None =>
      }
    }
    configuration
  }
}