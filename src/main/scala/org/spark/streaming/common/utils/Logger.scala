package org.spark.streaming.common.utils

import org.apache.log4j.Level
import org.apache.log4j.Level._
import org.slf4j.LoggerFactory

object Logger {

  val DELIMITER = "||"

  object FILE extends Enumeration {
    type FILE = String
    val DEFAULT = new String("DEFAULT")
  }

  private def mlogger(filename: String): org.slf4j.Logger = {
    LoggerFactory.getLogger(filename)
  }

  def log(filename: String, level: Level, obj: StructuredLog): Unit = {
    logger(filename, level, obj)
  }
  def log(filename: Class[_], level: Level, obj: StructuredLog): Unit = {
    logger(filename.getName, level, obj)
  }

  private def logger(filename: String, level: Level, obj: StructuredLog): Unit = {
    level match {
      case ERROR =>
        mlogger(filename).error(obj.toString, obj.error)
      case WARN =>
        mlogger(filename).warn(obj.toString, obj.error)
      case INFO =>
        mlogger(filename).info(obj.toString, obj.error)
      case _ =>
        mlogger(filename).debug(obj.toString, obj.error)
    }
  }


}