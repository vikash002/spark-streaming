package org.spark.streaming.common.config

import java.util.concurrent.atomic.AtomicBoolean

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.google.common.io.Resources
import org.apache.log4j.Level._
import org.spark.streaming.common.utils.{BaseSLog, Logger}

import scala.collection.immutable.HashMap

object Config {
  protected val isIntialized = new AtomicBoolean(false)
  private var configMap = Map[String, String]()
  @transient
  private final val yamlMapper = new YAMLMapper()
  yamlMapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  def init(file: String): Unit = {
    if(isIntialized.compareAndSet(false, true)) {
      Logger.log(this.getClass, INFO, BaseSLog("Config Initializing..."))
      configMap = yamlMapper.readValue(Resources.getResource(file), new TypeReference[HashMap[String, String]](){})
      Logger.log(this.getClass, INFO, BaseSLog("Config Initializing finished"))
    }
  }

  def getProperty(props: String): Option[String] = configMap.get(props) match {
    case Some(x) => Some(x.toString)
    case _ => None
  }

  def getMap(): Map[String, String] = {
    configMap
  }

}