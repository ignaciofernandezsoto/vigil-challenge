package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.sql.SparkSession

class SparkSessionWrapper(sparkSession: SparkSession) {

  val sparkContext: SparkContextWrapper = new SparkContextWrapper(sparkSession.sparkContext)

  def read: DataFrameReaderWrapper = new DataFrameReaderWrapper(sparkSession.read)

  def createDataset[T : EncoderWrapper](data: Seq[T])(implicit encoderWrapper: EncoderWrapper[T]): DataFrameWrapper[T] =
    new DataFrameWrapper(sparkSession.createDataset(data)(encoderWrapper.get))

}
