package org.spark.streaming.common

trait EmbeddedDependencies extends ConfigurationForTest with EmbeddedSpark with EmbeddedES{
  override def beforeAll() {
    super.beforeAll()
  }
  override def afterAll() {
    super.afterAll()
  }
}