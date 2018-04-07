package org.spark.streaming.common.utils

import java.util

import scala.collection.JavaConverters._

object Utility {

  final val defaultRetries: Int = 3
  final val EMPTY_STRING = ""
  private lazy val envProperties = new util.HashMap[String, String](System.getenv()).asScala

  def doRetry[T](numOfRetries: Int)(body: => T): T = {
    var retry = 0
    var sleep = 100
    var myError: Throwable = null
    while (retry < numOfRetries) {
      try {
        val a = body
        return a
      }
      catch {
        case e: Throwable =>
          myError = e
          retry = retry + 1
          sleep = sleep * 2
          Thread.sleep(sleep)
      }
    }
    throw new Throwable("RETRY_FAILED", myError)
  }


  def isNullOrEmpty(o: Any): Boolean = o match {
    case m: Map[_, _] => m.isEmpty
    case i: Iterable[Any] => i.isEmpty
    case null | None | EMPTY_STRING => true
    case Some(x) => isNullOrEmpty(x)
    case _ => false
  }

  private[utils] def overrideEnvironmentVariable(key: String, value: String) = {
    envProperties.put(key, value)
  }
}
