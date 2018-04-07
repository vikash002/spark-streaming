package org.spark.streaming.common.sparkjob.sink

import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.rdd.EsSpark
import org.spark.streaming.common.GenericSink
import org.spark.streaming.common.sparkjob.model.{DriverEventSnapShot, GenericSnapShot, UserEventSnapShot}
import org.spark.streaming.common.utils.JsonUtility

object EsSinkUserSnapShot  extends GenericSink[GenericSnapShot] {
  private val esConfig = Map(ES_MAPPING_ID -> "event_id",
    ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
    ES_BATCH_SIZE_ENTRIES -> "1000",
    ES_UPDATE_RETRY_ON_CONFLICT -> "10",
    ES_BATCH_WRITE_RETRY_COUNT -> "3",
    ES_INPUT_JSON -> "true",
    ES_BATCH_WRITE_REFRESH -> "true")

  override def save(models: RDD[GenericSnapShot]): Unit = {
    val modelJSONs = models.filter(_.userId != null).map(JsonUtility().serializePlain(_))
    EsSpark.saveToEs(modelJSONs, esConfig)
  }
}

object EsSinkDriverSnapShot extends GenericSink[DriverEventSnapShot] {
  private val esConfig = Map(ES_MAPPING_ID -> "event_id",
    ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
    ES_BATCH_SIZE_ENTRIES -> "1000",
    ES_UPDATE_RETRY_ON_CONFLICT -> "10",
    ES_BATCH_WRITE_RETRY_COUNT -> "3",
    ES_INPUT_JSON -> "true",
    ES_BATCH_WRITE_REFRESH -> "true")
  override def save(models: RDD[DriverEventSnapShot]): Unit = {
    val modelJSONs = models.filter(_.userId != null).map(JsonUtility().serializePlain(_))
    EsSpark.saveToEs(modelJSONs, esConfig)
  }
}
