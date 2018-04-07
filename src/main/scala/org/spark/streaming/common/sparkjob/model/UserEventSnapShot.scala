package org.spark.streaming.common.sparkjob.model

import org.spark.streaming.common.{Enrichable, Enricher, Uniqueness}

abstract class GenericSnapShot(val userId: String,
                               val timeStamp: Long) extends Serializable {
}

case class UserEventSnapShot(override val userId: String,
                             var estimatedDistanceInMtr: Double,
                             var pickUpLocation: Location,
                             var dropLocation: Location,
                             var pickUpGeoHash: String,
                             var dropGeoHash: String,
                             var estimatedTripDurationInMinute: Double,
                             override val timeStamp: Long) extends GenericSnapShot(userId, timeStamp) with Enrichable[UserEventSnapShot] with Uniqueness[Long]{

  override def uniqueBy = timeStamp

  override def enrichMe(enricher: Enricher[UserEventSnapShot]) = {
    enricher.enrich(this)
    this
  }
}


case class DriverEventSnapShot(override val userId: String,
                               var location: Location,
                               var locationGeoHash: String,
                               override val timeStamp: Long) extends GenericSnapShot(userId, timeStamp) with Enrichable[DriverEventSnapShot] with Uniqueness[Long]{
  override def enrichMe(enricher: Enricher[DriverEventSnapShot]) = {
    enricher.enrich(this)
    this
  }

  override def uniqueBy = timeStamp
}

case class Location(lat: Double, lon: Double)

case class WeatherInfo(tempInCelsius: Double,
                       description: Option[String],
                       pressure: Option[Double],
                       humidity: Option[Double],
                       windSpeedInKm: Option[Double],
                       clouds: Option[Double],
                       visibility: Option[Double],
                       geoHash: String,
                       override val timeStamp: Long) extends GenericSnapShot(geoHash, timeStamp) with Uniqueness[String] {

  override def uniqueBy = geoHash
}
