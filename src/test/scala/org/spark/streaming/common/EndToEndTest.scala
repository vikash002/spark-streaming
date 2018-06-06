package org.spark.streaming.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.spark.streaming.common.sparkjob.model.{DriverEventPayload, UserEventPayload}
import org.spark.streaming.common.sparkjob.service.{DriverTaskTransformation, UserTaskTransformation}
import org.spark.streaming.common.sparkjob.sink.{GenericSnapshotSink, GenericSnapshotsSink}
import org.spark.streaming.common.utils.JsonUtility

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by vikash.kr on 06/05/18.
  */
class EndToEndTest extends FunSuite with BeforeAndAfterEach with EmbeddedDependencies{

  private val mapper = new ObjectMapper()
  override def beforeEach(): Unit = {

  }
  test("user test") {
    val userPayload = getUserPayloads("userData.txt")
    val payloadJson = sparkContext.makeRDD(userPayload)

    val payloads = JsonUtility(mapper).deserialize[UserEventPayload](payloadJson)
    val transformedPayload = UserTaskTransformation.transformRDD(payloads)

    GenericSnapshotSink.save(transformedPayload)
    //val readFromEs = EsSpark.esJsonRDD(sparkContext, s"$esIndex/$userEsIndexType")
    //println(userPayload)
  }
  test("driver test") {
    val driverPayload = getUserPayloads("driverData.txt")
    val payloadJson = sparkContext.makeRDD(driverPayload)
    val payloads = JsonUtility(mapper).deserialize[DriverEventPayload](payloadJson)
    val transformedPayload = DriverTaskTransformation.transform(payloads)
    GenericSnapshotSink.save(transformedPayload)
    println("hello")

  }

  def getUserPayloads(fileName: String): Array[String] = {
    var res = new ListBuffer[String]
    val file = Resources.getResource(fileName).getFile
    val source = Source.fromFile(file)
    for(line <- source.getLines()) {
      res += line
    }
    res.toArray
  }

}
