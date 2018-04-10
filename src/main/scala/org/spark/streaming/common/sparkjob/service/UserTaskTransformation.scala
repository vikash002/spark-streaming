package org.spark.streaming.common.sparkjob.service

import org.spark.streaming.common.AbstractModelTransformer
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model.{GenericSnapShot, Location, UserEventPayload, UserEventSnapShot}

object UserTaskTransformation extends AbstractModelTransformer[GenericSnapShot, UserEventPayload](userBookingEventPartitions) {

  override def transform(payload: UserEventPayload) = {
    val pickUpLocation = Location(payload.pickUpLat, payload.pickUpLon)
    val dropLocation = Location(payload.dropLat, payload.dropLon)
    val estimatedTime = if(payload.dropTime.isDefined)  (payload.dropTime.get - payload.pickUpTime) else 0.0

    var snapShot = new UserEventSnapShot(payload.userId, 0.0, pickUpLocation, dropLocation, null, null, estimatedTime, payload.pickUpTime)
    snapShot.enrichMe(new GeoHashEnricherUserSnapShot(
      Some(new DistanceEnricher(None))
    ))
    snapShot
  }
}
