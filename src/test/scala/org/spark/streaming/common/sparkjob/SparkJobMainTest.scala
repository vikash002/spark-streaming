package org.spark.streaming.common.sparkjob

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.spark.streaming.common.EmbeddedDependencies

class SparkJobMainTest extends FunSuite with BeforeAndAfterEach with EmbeddedDependencies{

  override def beforeEach(): Unit = {
    dropESIndex()
  }

  test("Should test first Test Case") {

  }
}
