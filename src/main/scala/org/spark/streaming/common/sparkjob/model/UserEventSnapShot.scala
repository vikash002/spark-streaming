package org.spark.streaming.common.sparkjob.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.spark.streaming.common.sparkjob.entity.PayloadEntity.{PayloadEntityType, UserSnapshot, DriverSnapshot, DemandSupplySnapshot, WeatherSnapshot}
import org.spark.streaming.common.{Enrichable, Enricher, Uniqueness}

@JsonInclude(Include.NON_NULL)
abstract class GenericSnapShot(var userId: String,
                               var timeStamp: Long) extends Uniqueness[Long] with Serializable {
  @JsonIgnore
  def getType:PayloadEntityType
}

@JsonInclude(Include.NON_NULL)
class UserEventSnapShot( userId: String,
                         var estimatedDistanceInMtr: Double,
                         var pickUpLocation: Location,
                         var dropLocation: Location,
                         var pickUpGeoHash: String,
                         var dropGeoHash: String,
                         var estimatedTripDurationInMinute: Double,
                         timeStamp: Long) extends GenericSnapShot(userId, timeStamp) with Enrichable[UserEventSnapShot]{

  override def uniqueBy = timeStamp

  override def enrichMe(enricher: Enricher[UserEventSnapShot]) = {
    enricher.enrich(this)
    this
  }

  override def getType: PayloadEntityType = UserSnapshot
}

@JsonInclude(Include.NON_NULL)
class DriverEventSnapShot(userId: String,
                               var location: Location,
                               var locationGeoHash: String,
                               timeStamp: Long) extends GenericSnapShot(userId, timeStamp) with Enrichable[DriverEventSnapShot]{
  override def enrichMe(enricher: Enricher[DriverEventSnapShot]) = {
    enricher.enrich(this)
    this
  }

  override def uniqueBy = timeStamp

  override def getType: PayloadEntityType = DriverSnapshot
}

@JsonInclude(Include.NON_NULL)
case class Location(lat: Double, lon: Double)

@JsonInclude(Include.NON_NULL)
class WeatherInfo(tempInCelsius: Double,
                       description: Option[String],
                       pressure: Option[Double],
                       humidity: Option[Double],
                       windSpeedInKm: Option[Double],
                       clouds: Option[Double],
                       visibility: Option[Double],
                       geoHash: String,
                       timeStamp: Long) extends GenericSnapShot(geoHash, timeStamp) {

  override def uniqueBy = timeStamp

  override def getType: PayloadEntityType = WeatherSnapshot
}

@JsonInclude(Include.NON_NULL)
class DemandSupply(id: String,
                   var geoHash: String,
                   timeStamp: Long,
                   var userCount: Long,
                   var driverCount: Long
                  ) extends GenericSnapShot(id, timeStamp){

  override def uniqueBy: Long = timeStamp

  override def getType: PayloadEntityType = DemandSupplySnapshot


  override def toString = s"DemandSupply()"
}