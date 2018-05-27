package org.spark.streaming.common.service


import org.apache.spark.HashPartitioner
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

class RDDPartitionerWithKeyed[Key: ClassTag, Payload <: Uniqueness[Key] : ClassTag](private val numPartitions: Int) {

  def partition(rdd: RDD[Payload]): RDD[Payload] = {
    if (numPartitions > 1) {
      val hashPartitioner = new HashPartitioner(numPartitions)
      rdd.keyBy(x => x.uniqueBy).partitionBy(hashPartitioner).map(x => x._2)
    } else {
      rdd
    }
  }
}
