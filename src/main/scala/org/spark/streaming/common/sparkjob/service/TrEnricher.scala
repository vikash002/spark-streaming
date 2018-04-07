package org.spark.streaming.common.sparkjob.service

import org.elasticsearch.common.geo.GeoHashUtils
import org.spark.streaming.common.sparkjob.model._
import org.spark.streaming.common.{AbstractEnricher, Enricher}

class DistanceEnricher(val successor: Option[Enricher[UserEventSnapShot]]) extends AbstractEnricher[UserEventSnapShot](successor) {
  override def enrichModel(model: UserEventSnapShot): Unit = {
    model.estimatedDistanceInMtr = DistanceCalculatorImpl.calculateDistanceInMtr(model.pickUpLocation, model.dropLocation)
  }
}

class GeoHashEnricherUserSnapShot(val successor: Option[Enricher[UserEventSnapShot]]) extends AbstractEnricher[UserEventSnapShot](successor) {

  override def enrichModel(model: UserEventSnapShot): Unit = {
    model.pickUpGeoHash = enrichGeoHash(model.pickUpLocation)
    model.dropGeoHash = enrichGeoHash(model.dropLocation)
  }
}

class GeoHashEnricherDriverSnapShot(val successor: Option[Enricher[DriverEventSnapShot]]) extends AbstractEnricher[DriverEventSnapShot](successor) {

  override def enrichModel(model: DriverEventSnapShot): Unit = {
    model.locationGeoHash = enrichGeoHash(model.location)
  }
}

object enrichGeoHash {
  def apply(location: Location): String = {
    GeoHashUtils.stringEncode(location.lon, location.lat, 6)
  }
}

object WeatherInfoEnricher {
  def apply(location: Location): WeatherInfo = {
    //TODO implementation
    null
  }
}

