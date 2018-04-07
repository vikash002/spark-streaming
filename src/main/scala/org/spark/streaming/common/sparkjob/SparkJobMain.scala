package org.spark.streaming.common.sparkjob

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Level._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.spark.streaming.common.config.Config
import org.spark.streaming.common.service.SparkJobBuilder
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model.{GenericSnapShot, UserEventPayload}
import org.spark.streaming.common.sparkjob.service.UserTaskTransformation
import org.spark.streaming.common.sparkjob.sink.EsSinkUserSnapShot
import org.spark.streaming.common.utils.{BaseSLog, Logger}

object SparkJobMain extends App {
  Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))

  if(args.length < 1) {
    Logger.log(this.getClass, INFO, BaseSLog(s"Must provide configuration file path"))
    throw new RuntimeException("Must provide configuration file path")
  }
  private val filePath = args(0)
  Config.init(filePath)
  val ssc = new StreamingContext(sparkConfiguration, Seconds(streamingInterval))
  try {
    val mapper = new ObjectMapper()
    new SparkJobBuilder[GenericSnapShot, UserEventPayload]().withObjectMapper(mapper)
      .from(userBookingtEventKafkaTopic).using(UserTaskTransformation).to(EsSinkUserSnapShot)
      .build(ssc)
      .run()
  } catch {
    case e: Exception => {
      Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing SFCMain: ${e.getMessage}", e))
      throw e
    }
  } finally sys.ShutdownHookThread {
    Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping SFCMain"))
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    Logger.log(this.getClass, ERROR, BaseSLog(s"SFCMain stopped"))
  }
}
