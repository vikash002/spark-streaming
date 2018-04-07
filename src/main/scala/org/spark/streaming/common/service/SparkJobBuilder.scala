package org.spark.streaming.common.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.streaming.StreamingContext
import org.spark.streaming.common.config.ZookeeperConfiguration
import org.spark.streaming.common.messaging.KafkaStreamSource
import org.spark.streaming.common._

class SparkJobBuilder[Model, Payload] {
  private var groupId: String = _
  private var topic: String = _
  private var transformer: ModelTransformer[Model, Payload] = _
  private var modelSink: GenericSink[Model] = _
  private var sink: Sink[Payload, Model] = _
  private var objectMapper: ObjectMapper = _
  private var batchCompletionHandler: Long => Unit = (_: Long) => {}

  def from(topic: String): SparkJobBuilder[Model, Payload] = {
    this.topic = topic
    this
  }

  def using(transformer: ModelTransformer[Model, Payload]): SparkJobBuilder[Model, Payload] = {
    this.transformer = transformer
    this
  }

  def to(modelSink: GenericSink[Model]): SparkJobBuilder[Model, Payload] = {
    this.modelSink = modelSink
    this
  }

  def to(sink: Sink[Payload, Model]): SparkJobBuilder[Model, Payload] = {
    this.sink = sink
    this
  }

  def withObjectMapper(objectMapper: ObjectMapper): SparkJobBuilder[Model, Payload] = {
    this.objectMapper = objectMapper
    this
  }

  def withBatchCompletionHandler(batchCompletionHandler: Long => Unit): SparkJobBuilder[Model, Payload] = {
    this.batchCompletionHandler = batchCompletionHandler
    this
  }

  def withGroupId(groupId: String): SparkJobBuilder[Model, Payload] = {
    this.groupId = groupId
    this
  }

  def build(ssc: StreamingContext): KafkaSparkJob[Model, Payload] =  {
    if(sink == null && modelSink == null) throw new IllegalArgumentException("Sink and Model Sink Must Not Null")
    if(transformer == null) throw new IllegalArgumentException("Must provide a transformer")
    if(topic == null) throw new IllegalArgumentException("Must Provide a Topic")
    val consumerGroupId = Option(this.groupId).getOrElse(ZookeeperConfiguration.ZKGroupId)
    val zkProperties = Map("group.id" -> consumerGroupId, "zookeeper.connect" -> ZookeeperConfiguration.zkHost)
    val kafkaStreamSource = new KafkaStreamSource(ssc, topic, zkProperties)
    val checkPointer = Option(new CheckPointer(zkProperties))
    val genericSink = if (sink != null) sink else new SimpleSink[Payload, Model](None, Some(modelSink))
    new KafkaSparkJob[Model, Payload](kafkaStreamSource, transformer, genericSink, Option(objectMapper),
      batchCompletionHandler, checkPointer)
  }

}