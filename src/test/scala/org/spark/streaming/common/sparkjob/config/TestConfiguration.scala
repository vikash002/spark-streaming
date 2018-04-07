package org.spark.streaming.common.sparkjob.config

object TestConfiguration {

  def withConfiguration(configs: Map[String, String]) = {

    Configuration.setPropertyLoader((s: String) => None)
    for (config <- configs) {
      Configuration.overrideConfig(config._1, config._2)
    }
  }
}