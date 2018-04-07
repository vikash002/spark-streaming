package org.spark.streaming.common.sparkjob.service

import org.spark.streaming.common.sparkjob.model.Location

trait DistanceCalculator {
  def calculateDistanceInMtr(fromLocation: Location, toLocation: Location): Double
}

object DistanceCalculatorImpl extends DistanceCalculator {
  private val Average_Earth_Radius_In_KM = 6371

  override def calculateDistanceInMtr(fromLocation: Location, toLocation: Location) = {
    val latDistance = Math.toRadians(fromLocation.lat - toLocation.lat)
    val lonDistance = Math.toRadians(fromLocation.lon - toLocation.lon)
    val sinLat = Math.sin(latDistance/2)
    val sinLon = Math.sin(lonDistance/2)

    val disS = sinLat*sinLat + (Math.cos(Math.toRadians(fromLocation.lat))*Math.cos(Math.toRadians(toLocation.lat))*sinLon*sinLon)
    val tangent = 2*Math.atan2(Math.sqrt(disS), Math.sqrt(1 - disS))
    (Average_Earth_Radius_In_KM*tangent*1000).toInt
  }
}
