package org.spark.streaming.common

import org.apache.spark.rdd.RDD
import org.spark.streaming.common.messaging.ZookeeperManager

import scala.reflect.ClassTag

trait GenericSink[Model] {
  def save(model: RDD[Model])
}

trait Sink[Payload, Model] {
  def save(payloads: RDD[Payload], model: RDD[Model]): Unit
}

class SimpleSink[Payload, Model](private val payloadSink: Option[GenericSink[Payload]], private val modelSink: Option[GenericSink[Model]])
  extends Sink[Payload, Model] {
  override def save(payloads: RDD[Payload], model: RDD[Model]): Unit = {
    payloadSink.foreach(_.save(payloads))
    modelSink.foreach(_.save(model))
  }
}

class CheckPointer(private final val zkProperties: Map[String, String]) extends GenericSink[(String, String)] {
  override def save(messagesByKey: RDD[(String, String)]): Unit = {
    ZookeeperManager.updateOffsetInZk(messagesByKey, zkProperties)
  }
}

class AggregateSink[Model: ClassTag](private val componentSink: GenericSink[Model]) extends GenericSink[List[Model]] {
  override def save(models: RDD[List[Model]]): Unit = {
    componentSink.save(flatten(models))
  }
  protected def flatten(models: RDD[List[Model]]): RDD[Model] = {
    models.flatMap(model => model)
  }
}

