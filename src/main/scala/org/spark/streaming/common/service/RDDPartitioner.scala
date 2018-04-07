package org.spark.streaming.common.service

import org.apache.spark.rdd.RDD
import org.spark.streaming.common.Uniqueness

import scala.math.Ordering.by
import scala.reflect.ClassTag

class RDDPartitioner[Payload <: Uniqueness[_]: ClassTag](private val numberOfPartition: Int) {

  def partition(rdd: RDD[Payload]): RDD[Payload] = {
    if(numberOfPartition > 1)
      rdd.repartition(numberOfPartition)(by(_.uniqueBy.toString))
    else rdd
  }
}
