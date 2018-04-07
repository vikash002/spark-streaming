package org.spark.streaming.common.utils

import java.io.{IOException, StringWriter}
import java.lang.reflect.{ParameterizedType, Type}
import java.util.concurrent.atomic.AtomicBoolean

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.Level._
import org.apache.spark.rdd.RDD

class JsonUtility(mapper: ObjectMapper) extends Serializable {

  def serialize(value: Any): String = {
    registerScalaModuleIfNeeded
    val writer = new StringWriter()
    mapper.writeValue(writer, value)
    writer.toString
  }

  def serializePlain(value: Any): String = {
    registerScalaModuleIfNeeded
    val writer = new StringWriter()
    mapper.writeValue(writer, value)
    writer.toString
  }

  def deserialize[T: Manifest](value: String): Option[T] =
    if (value == null) None
    else try {
      registerScalaModuleIfNeeded
      Option(mapper.readValue(value, typeReference[T]))
    } catch {
      case e: IOException =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"An error occurred while de-serializing JSON payload: $value", e))
        None
    }

  def deserialize[T: Manifest](values: RDD[String]): RDD[T] =
      values.mapPartitions(xs => xs.map(x => deserialize(x))).filter(x => x.isDefined)
        .mapPartitions(options => options.map(_.get))

  def pretty(json: String): String = {
    val tree = mapper.readTree(json)
    mapper.writeValueAsString(tree)
  }

  private [this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  private [this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
  }

  private val scalaModuleRegistered = new AtomicBoolean(false)

  private def registerScalaModuleIfNeeded = {
    if (scalaModuleRegistered.compareAndSet(false, true)) {
      mapper.registerModule(DefaultScalaModule)
    }
  }
}

object JsonUtility {
  def apply(mapperOption: Option[ObjectMapper]): JsonUtility = {
    mapperOption match {
      case Some(mapper) => apply(mapper)
      case None => apply()
    }
  }

  def apply(mapper: ObjectMapper): JsonUtility = {
    new JsonUtility(mapper)
  }

  def apply(): JsonUtility = {
    val mapper = new ObjectMapper()
    mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true)
    mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true)
    apply(mapper)
  }
}