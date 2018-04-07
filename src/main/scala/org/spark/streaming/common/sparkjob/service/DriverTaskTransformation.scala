package org.spark.streaming.common.sparkjob.service

import org.spark.streaming.common.AbstractModelTransformer
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model.{DriverEventPayload, DriverEventSnapShot, Location}

object DriverTaskTransformation extends AbstractModelTransformer[DriverEventSnapShot, DriverEventPayload](driverEventPartition){
  override def transform(payload: DriverEventPayload) = {

    val location = Location(payload.locationLat, payload.locationLon)
    var driverEventSnapShot = new DriverEventSnapShot(payload.driverId, location, null, payload.timeStamp)
    driverEventSnapShot.enrichMe(
      new GeoHashEnricherDriverSnapShot(None)
    )
    driverEventSnapShot
  }
}
