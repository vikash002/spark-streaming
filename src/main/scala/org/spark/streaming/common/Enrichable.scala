package org.spark.streaming.common

trait Enrichable[Model] {
 def enrichMe(enricher: Enricher[Model]): Model
}
