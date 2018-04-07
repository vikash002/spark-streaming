package org.spark.streaming.common.sparkjob.model

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.spark.streaming.common.Uniqueness

@JsonInclude(Include.NON_NULL)
class UserEventPayload(var userId: String,
                       var pickUpLat: Double,
                       var pickUpLon: Double,
                       var dropLat: Double,
                       var dropLog: Double,
                       var pickUpTime: Long,
                       var dropTime: Option[Long]) extends Uniqueness[String] with Serializable {
  override def uniqueBy = userId
}

@JsonInclude(Include.NON_NULL)
class DriverEventPayload(var driverId: String,
                         var locationLat: Double,
                         var locationLon: Double,
                         var timeStamp: Long) extends Uniqueness[Long] with Serializable {
  override def uniqueBy = timeStamp
}
