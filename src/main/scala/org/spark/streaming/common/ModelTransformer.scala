package org.spark.streaming.common

import org.apache.spark.rdd.RDD
import org.spark.streaming.common.service.RDDPartitioner

import scala.reflect.ClassTag

trait ModelTransformer[Model, Payload] extends Serializable {
  def transformRDD(payloads: RDD[Payload]): RDD[Model]
}

abstract class AbstractPartitionModelTransformer[Model: ClassTag, Payload <: Uniqueness[_]: ClassTag]
  (private val numberOfPartition: Int) extends ModelTransformer[Model, Payload] {

  def transform(payload: RDD[Payload]): RDD[Model]

  override def transformRDD(rdd: RDD[Payload]): RDD[Model] = {
    transform(new RDDPartitioner[Payload](numberOfPartition).partition(rdd))
  }
}

abstract class AbstractModelTransformer[Model: ClassTag, Payload <: Uniqueness[_] : ClassTag]
(private final val numPartitions: Int) extends AbstractPartitionModelTransformer[Model, Payload](numPartitions) {

  def transform(payload: Payload): Model

  override def transform(payloads: RDD[Payload]): RDD[Model] = {
    payloads.mapPartitions(payloadPartition => { payloadPartition.map(transform) })
  }
}