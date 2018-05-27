package org.spark.streaming.common.sparkjob

import org.apache.log4j.Level._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.spark.streaming.common.config.Config
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.config.Job
import org.spark.streaming.common.utils.{BaseSLog, Logger}

object SparkJobMain extends App {
  if(args.length < 1) {
    Logger.log(this.getClass, INFO, BaseSLog(s"Must provide configuration file path"))
    throw new RuntimeException("Must provide configuration file path")
  }
  private val filePath = args(0)
  Config.init(filePath)
  Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
  val ssc = new StreamingContext(sparkConfiguration, Seconds(streamingInterval))
  try {
    val jobs = if(args.length < 2) Job.values.toArray else args(1).split(",").map(Job.withName)
    for(job <- jobs) {
      job.run(ssc)
    }
    ssc.start()
    ssc.awaitTermination()
  } catch {
    case e: Exception => {
      Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing SparkMain: ${e.getMessage}", e))
      throw e
    }
  } finally sys.ShutdownHookThread {
    Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping SparkMain"))
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    Logger.log(this.getClass, ERROR, BaseSLog(s"Spark stopped"))
  }
}
