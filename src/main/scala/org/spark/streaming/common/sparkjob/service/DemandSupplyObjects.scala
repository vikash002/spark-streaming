package org.spark.streaming.common.sparkjob.service

import java.util.concurrent.TimeUnit

import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.entity.PayloadEntity.{DriverSnapshot, UserSnapshot}
import org.spark.streaming.common.sparkjob.model._
/**
  * Created by vikash.kr on 14/05/18.
  */
object DemandSupplyObjects {
  val time = System.currentTimeMillis()

  def apply(geoHash: String, size: Int, driver: Boolean): GenericSnapShot = {
    val t = TimeUnit.MILLISECONDS.toMinutes(time)*demandSupplyAggInterval
    val res = new DemandSupply(t.toString + '@'  + geoHash, geoHash, time, 0, 0)
    if(driver) res.driverCount += size
    else res.userCount += size
    res
  }

}
