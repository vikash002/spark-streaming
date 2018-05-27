package org.spark.streaming.common.sparkjob.entity

/**
  * Created by vikash.kr on 14/05/18.
  */
object PayloadEntity extends Enumeration {
  type PayloadEntityType = Value
  val DriverEvent, UserEvent, UserSnapshot, DriverSnapshot, WeatherSnapshot, DemandSupplySnapshot = Value
}
