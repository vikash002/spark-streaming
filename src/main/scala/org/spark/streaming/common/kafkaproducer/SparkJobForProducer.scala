package org.spark.streaming.common.kafkaproducer

import org.apache.log4j.Level._
import org.apache.spark.sql.SparkSession
import org.spark.streaming.common.config.Config
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.utils.{BaseSLog, Logger}
/**
  * Created by vikash.kr on 08/04/18.
  */
object SparkJobForProducer extends App{
  if(args.length < 1) {
    Logger.log(this.getClass, ERROR, BaseSLog(s"Must provide file name"))
  }
  val fileName = args(0)
  val readFilePath = Config.getProperty("spark.producer.fileName")

  val ss = SparkSession.builder().config(sparkConfiguration).getOrCreate()
}
