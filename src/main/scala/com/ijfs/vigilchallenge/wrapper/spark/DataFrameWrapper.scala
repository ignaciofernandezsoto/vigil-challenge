package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.sql.{Dataset, Encoder}

class DataFrameWrapper[T](dataFrame: Dataset[T]) {

  private[spark] def get = dataFrame

  def map[U : EncoderWrapper](func: T => U)(implicit encoder: EncoderWrapper[U]): DataFrameWrapper[U] =
    new DataFrameWrapper(dataFrame.map(func)(encoder.get))

  def union(other: DataFrameWrapper[T]): DataFrameWrapper[T] =
    new DataFrameWrapper(dataFrame.union(other.get))

  def toLocalIterator(): java.util.Iterator[T] = dataFrame.toLocalIterator()

  lazy val rdd: RDDWrapper[T] = new RDDWrapper(dataFrame.rdd)

}
