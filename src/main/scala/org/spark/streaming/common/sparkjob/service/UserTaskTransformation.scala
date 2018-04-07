package org.spark.streaming.common.sparkjob.service

import org.spark.streaming.common.AbstractModelTransformer
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model.{GenericSnapShot, Location, UserEventPayload, UserEventSnapShot}

object UserTaskTransformation extends AbstractModelTransformer[GenericSnapShot, UserEventPayload](userBookingEventPartitions) {

  override def transform(payload: UserEventPayload) = {
    val pickUpLocation = Location(payload.pickUpLat, payload.pickUpLon)
    val dropLocation = Location(payload.dropLat, payload.dropLog)
    var snapShot = new UserEventSnapShot(payload.userId, 0.0, pickUpLocation, dropLocation, null, null, 0.0, payload.pickUpTime)
    snapShot.enrichMe(new GeoHashEnricherUserSnapShot(
      Some(new DistanceEnricher(None))
    ))
    snapShot
  }
}
