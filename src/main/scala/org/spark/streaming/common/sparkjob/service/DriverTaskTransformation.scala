package org.spark.streaming.common.sparkjob.service

import org.apache.spark.rdd.RDD
import org.spark.streaming.common.ModelTransformer
import org.spark.streaming.common.service.RDDPartitioner
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model._

object DriverTaskTransformation extends ModelTransformer[GenericSnapShot, DriverEventPayload] {

  override def transformRDD(payloads: RDD[DriverEventPayload]): RDD[GenericSnapShot] = {
    transform(new RDDPartitioner[DriverEventPayload](driverEventPartition).partition(payloads))
  }

  def transform(payloads: RDD[DriverEventPayload]): RDD[GenericSnapShot] = {
    val userEventsRDD= payloads.map(getDriverPayload(_))
    val demand = userEventsRDD.groupBy(_.asInstanceOf[DriverEventSnapShot].locationGeoHash).map(f => DemandSupplyObjects(f._1, f._2.size, true))

    userEventsRDD ++ demand
  }

  def getDriverPayload(payload: DriverEventPayload): GenericSnapShot = {
    val location = Location(payload.locationLat, payload.locationLon)
    val driverEventSnapShot = new DriverEventSnapShot(payload.driverId, location, null, payload.timeStamp)
    driverEventSnapShot.enrichMe(
      new GeoHashEnricherDriverSnapShot(None)
    )
    driverEventSnapShot
  }
}
