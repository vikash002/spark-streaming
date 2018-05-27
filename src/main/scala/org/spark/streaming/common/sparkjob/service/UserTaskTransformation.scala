package org.spark.streaming.common.sparkjob.service

import org.apache.spark.rdd.RDD
import org.spark.streaming.common.ModelTransformer
import org.spark.streaming.common.service.RDDPartitioner
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model._

object UserTaskTransformation extends ModelTransformer[GenericSnapShot, UserEventPayload] {

  override def transformRDD(payloads: RDD[UserEventPayload]): RDD[GenericSnapShot] = {
    transform(new RDDPartitioner[UserEventPayload](userBookingEventPartitions).partition(payloads))
  }

  def transform(payloads: RDD[UserEventPayload]): RDD[GenericSnapShot] = {

    val userEventsRDD= payloads.map(getUserPayload(_))
    val demand = userEventsRDD.groupBy(_.asInstanceOf[UserEventSnapShot].pickUpGeoHash).map(f => DemandSupplyObjects(f._1, f._2.size, false))

    userEventsRDD ++ demand
  }

  private def getUserPayload(payload: UserEventPayload): GenericSnapShot = {

    val pickUpLocation = Location(payload.pickUpLat, payload.pickUpLon)
    val dropLocation = Location(payload.dropLat, payload.dropLon)
    val estimatedTime = if(payload.dropTime != null)  (payload.dropTime - payload.pickUpTime) else 0.0
    val snapShot = new UserEventSnapShot(payload.userId, 0.0, pickUpLocation, dropLocation, null, null, estimatedTime, payload.pickUpTime)
    snapShot.enrichMe(new GeoHashEnricherUserSnapShot(
      Some(new DistanceEnricher(None))
    ))
    snapShot
  }

}
