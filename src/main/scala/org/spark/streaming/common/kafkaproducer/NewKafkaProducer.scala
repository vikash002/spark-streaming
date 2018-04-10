package org.spark.streaming.common.kafkaproducer

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata, KafkaProducer}
import org.spark.streaming.common.sparkjob.config.Configuration._

import java.util.concurrent.Future

/**
  * Created by vikash.kr on 08/04/18.
  */

class NewKafkaProducer[K,V] (createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val lazyProducer: KafkaProducer[String, String] = {
    new KafkaProducer[String, String](kafkaProducerConfiguration)
  }

  def send(topic: String, key: String, value: String): Future[RecordMetadata] =
    lazyProducer.send(new ProducerRecord[String,String](topic, key, value))

  def send(topic: String, value: String): Future[RecordMetadata] =
    lazyProducer.send(new ProducerRecord[String,String](topic, value))
}

object NewKafkaProducer {
  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): NewKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new NewKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): NewKafkaProducer[K, V] = apply(config.toMap)

}
