package org.spark.streaming.common.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.spark.streaming.common.{CheckPointer, ModelTransformer, Sink}
import org.spark.streaming.common.messaging.KafkaStreamSource
import org.spark.streaming.common.utils.JsonUtility

class KafkaSparkJob[Model, Payload](private final val kafkaStreamSource: KafkaStreamSource,
                                    private final val modelTransformer: ModelTransformer[Model, Payload],
                                    private final val sink: Sink[Payload, Model],
                                    private final val objectMapper: Option[ObjectMapper],
                                    private final val batchCompletionHandler: Long => Unit,
                                    private final val checkPointer: Option[CheckPointer]) {

  def run()(implicit payloadManifest: Manifest[Payload]): Unit = {
    runStream(kafkaStreamSource.getStream)
  }

  private def runStream(dataStream: DStream[(String, String)])(implicit payloadManifest: Manifest[Payload]) = {
    dataStream.foreachRDD(rdd => runBatch(rdd))
  }

  private def runBatch(messagesByKey: RDD[(String, String)])(implicit payloadManifest: Manifest[Payload]): Unit = {
    val payloads = JsonUtility(objectMapper).deserialize[Payload](messagesByKey.mapPartitions(_.map(_._2)))
    val models = modelTransformer.transformRDD(payloads)
    sink.save(payloads, models)
    checkPointer.foreach(_.save(messagesByKey))
    batchCompletionHandler(System.currentTimeMillis())
  }
}