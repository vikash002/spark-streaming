package org.spark.streaming.common

trait Uniqueness[Key] {
  def uniqueBy: Key
}

trait Filterable[Key]

trait TemporalFilter[Key] extends Uniqueness[Key] with Filterable[Key] {
  def timestamp: Long
}

trait DeDuplicable[Key] extends Uniqueness[Key] {
  def deDuplicationKey: Key
}

