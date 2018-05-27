package org.spark.streaming.common.sparkjob.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.spark.streaming.common.Uniqueness
import org.spark.streaming.common.sparkjob.entity.PayloadEntity.{PayloadEntityType, UserEvent, DriverEvent}

trait PayloadEvent {
  @JsonIgnore
  def getType: PayloadEntityType
}
@JsonInclude(Include.NON_NULL)
class UserEventPayload(var userId: String,
                       var pickUpLat: Double,
                       var pickUpLon: Double,
                       var dropLat: Double,
                       var dropLon: Double,
                       var pickUpTime: Long,
                       var dropTime: Long) extends Uniqueness[String] with PayloadEvent with Serializable {
  override def uniqueBy = userId

  override def toString = s"UserEventPayload(userId=$userId, pickUpLat=$pickUpLat, pickUpLon=$pickUpLon, dropLat=$dropLat, dropLon=$dropLon, pickUpTime=$pickUpTime, dropTime=$dropTime)"

  override def getType: PayloadEntityType = UserEvent
}

@JsonInclude(Include.NON_NULL)
class DriverEventPayload(var driverId: String,
                         var locationLat: Double,
                         var locationLon: Double,
                         var timeStamp: Long) extends Uniqueness[Long] with PayloadEvent with Serializable {
  override def uniqueBy = timeStamp

  override def toString = s"DriverEventPayload(driverId=$driverId, locationLat=$locationLat, locationLon=$locationLon, timeStamp=$timeStamp)"

  @JsonIgnore
  override def getType: PayloadEntityType = DriverEvent
}
