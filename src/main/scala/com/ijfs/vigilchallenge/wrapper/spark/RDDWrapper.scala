package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDWrapper[T](rdd: RDD[T]) {

  def coalesce(numPartitions: Int) =
    new RDDWrapper(rdd.coalesce(numPartitions))

  def map[U: ClassTag](f: T => U): RDDWrapper[U] = new RDDWrapper(rdd.map(f))

  def saveAsTextFile(path: String): Unit = rdd.saveAsTextFile(path)

}
