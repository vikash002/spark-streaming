package org.spark.streaming.common.sparkjob.sink


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.rdd.EsSpark
import org.spark.streaming.common.service.RDDPartitioner
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.entity.PayloadEntity._
import org.spark.streaming.common.sparkjob.model._
import org.spark.streaming.common.utils.JsonUtility
import org.spark.streaming.common.{AggregateSink, GenericSink}
// HBASEBACETFEATURE

object GenericSnapshotSink extends GenericSink[GenericSnapShot] {
  override def save(model: RDD[GenericSnapShot]): Unit = {

    val usersPayloads = model.filter(_.getType == UserSnapshot).map(_.asInstanceOf[UserEventSnapShot])
    val driversPayloads = model.filter(_.getType == DriverSnapshot).map(_.asInstanceOf[DriverEventSnapShot])
    val demandSupply = model.filter(_.getType == DemandSupplySnapshot).map(_.asInstanceOf[DemandSupply])

    EsSinkDemandSupply.save(demandSupply)
    EsSinkDriverSnapShot.save(driversPayloads)
    EsSinkUserSnapShot.save(usersPayloads)
  }
}

object GenericSnapshotsSink extends AggregateSink[GenericSnapShot](GenericSnapshotSink){
  override protected def flatten(models: RDD[List[GenericSnapShot]]): RDD[GenericSnapShot] = {
    new RDDPartitioner[GenericSnapShot](modelPartitions).partition(super.flatten(models))
  }
}

object EsSinkDemandSupply extends GenericSink[DemandSupply] {
  val up_params = "new_driverCount: driverCount, new_userCount: userCount"
  val up_script = "ctx._source.driverCount += params.new_driverCount; ctx._source.userCount += params.new_userCount;"

  private val esConfig = Map(ES_MAPPING_ID -> "userId",
    ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
    ES_BATCH_SIZE_ENTRIES -> "10",
    ES_UPDATE_RETRY_ON_CONFLICT -> "10",
    ES_BATCH_WRITE_RETRY_COUNT -> "10",
    ES_INPUT_JSON -> "true",
    ES_UPDATE_SCRIPT_LANG -> "painless",
    ES_UPDATE_SCRIPT_PARAMS -> up_params,
    ES_UPDATE_SCRIPT_LEGACY -> up_script,
    ES_BATCH_WRITE_REFRESH -> "true")
  private val mapper = new ObjectMapper()

    override def save(model: RDD[DemandSupply]): Unit = {
    val modleJSONs = model.filter(_.userId != null).map(JsonUtility(mapper).serializePlain(_))
    EsSpark.saveToEs(modleJSONs, "%s/%s".format(demandSupplyEsIndex, demandSupplyEsIndexType), esConfig)
  }
}

object EsSinkUserSnapShot  extends GenericSink[UserEventSnapShot] {

  private val esConfig = Map(ES_MAPPING_ID -> "userId",
    ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
    ES_BATCH_SIZE_ENTRIES -> "1000",
    ES_UPDATE_RETRY_ON_CONFLICT -> "10",
    ES_BATCH_WRITE_RETRY_COUNT -> "3",
    ES_INPUT_JSON -> "true",
    ES_BATCH_WRITE_REFRESH -> "true")
  private val mapper = new ObjectMapper()

  override def save(models: RDD[UserEventSnapShot]): Unit = {
    val modelJSONs = models.filter(_.userId != null).map(JsonUtility(mapper).serializePlain(_))
    EsSpark.saveToEs(modelJSONs, "%s/%s".format(userEsIndex, userEsIndexType), esConfig)
  }
}

object EsSinkDriverSnapShot extends GenericSink[DriverEventSnapShot] {
  private val esConfig = Map(ES_MAPPING_ID -> "userId",
    ES_WRITE_OPERATION -> ES_OPERATION_UPSERT,
    ES_BATCH_SIZE_ENTRIES -> "1000",
    ES_UPDATE_RETRY_ON_CONFLICT -> "10",
    ES_BATCH_WRITE_RETRY_COUNT -> "3",
    ES_INPUT_JSON -> "true",
    ES_BATCH_WRITE_REFRESH -> "true")
  private val mapper = new ObjectMapper()
  override def save(models: RDD[DriverEventSnapShot]): Unit = {
    val modelJSONs = models.filter(_.userId != null).map(JsonUtility(mapper).serializePlain(_))
    EsSpark.saveToEs(modelJSONs,"%s/%s".format(driverEsIndex, driverEsIndexType), esConfig)
  }
}
