package org.spark.streaming.common.sparkjob.config

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.streaming.StreamingContext
import org.spark.streaming.common.service.SparkJobBuilder
import org.spark.streaming.common.sparkjob.config.Configuration._
import org.spark.streaming.common.sparkjob.model.{DriverEventPayload, GenericSnapShot, UserEventPayload}
import org.spark.streaming.common.sparkjob.service.{DriverTaskTransformation, UserTaskTransformation}
import org.spark.streaming.common.sparkjob.sink.{GenericSnapshotSink, GenericSnapshotsSink}

/**
  * Created by vikash.kr on 06/05/18.
  */
object Job extends Enumeration {
  protected abstract class JobVal() extends super.Val {
    def run(ssc: StreamingContext)
  }

  private final val mapper = new ObjectMapper()

  val USER_UPDATE = new JobVal {
    override def run(ssc: StreamingContext): Unit = {
      new SparkJobBuilder[GenericSnapShot, UserEventPayload]().withObjectMapper(mapper)
        .from(userBookingtEventKafkaTopic).using(UserTaskTransformation).to(GenericSnapshotSink)
        .build(ssc)
        .run()
    }
  }

  val DRIVER_UPDATE =  new JobVal {
    override def run(ssc: StreamingContext): Unit = {
      new SparkJobBuilder[GenericSnapShot, DriverEventPayload]().withObjectMapper(mapper)
        .from(driverEventKafkaTopic).using(DriverTaskTransformation).to(GenericSnapshotSink)
        .build(ssc)
        .run()
    }
  }
  implicit def convert(value: Value): JobVal = value.asInstanceOf[JobVal]

}
