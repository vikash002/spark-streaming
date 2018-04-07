package org.spark.streaming.common

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait EmbeddedSpark extends FunSuite with BeforeAndAfterAll {

  protected var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setAppName("ESInstance Spec").setMaster("local[1]")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")
      .set("es.http.timeout", "5m")
      .set("es.index.auto.create", "true")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.port", "61921")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
      .set("es.index.read.missing.as.empty", "true")
    sparkContext = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    super.afterAll()
  }
}
