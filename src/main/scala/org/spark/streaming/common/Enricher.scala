package org.spark.streaming.common

trait Enricher[Model] {
  def enrich(model: Model): Unit
}

abstract class AbstractEnricher[Model <: Enrichable[Model]] (private val successor: Option[Enricher[Model]])
  extends Enricher[Model] {

  override def enrich(model: Model): Unit = {
    enrichModel(model)
    if(successor.isDefined) {
      model.enrichMe(successor.get)
    }
  }
  def enrichModel(model: Model): Unit
}