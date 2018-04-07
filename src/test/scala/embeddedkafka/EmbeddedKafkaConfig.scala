package embeddedkafka

case class EmbeddedKafkaConfig(kafkaPort: Int, zooKeeperPort: Int, messageWaitingTimeInSecs: Int)

object EmbeddedKafkaConfig {
  implicit val defaultConfig = EmbeddedKafkaConfig(6001, 6000, 1)
}