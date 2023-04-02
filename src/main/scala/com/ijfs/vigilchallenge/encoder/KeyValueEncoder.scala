package com.ijfs.vigilchallenge.encoder

import com.ijfs.vigilchallenge.wrapper.spark.EncoderWrapper
import org.apache.spark.sql.Encoders.{STRING, tuple}

trait SparkEncoder[T, G[_]] {
  def encoder: G[T]
}

trait SparkRowEncoder extends SparkEncoder[(String, String), EncoderWrapper] {
  def encoder: EncoderWrapper[(String, String)] = new EncoderWrapper(tuple[String, String](STRING, STRING))
}

object SparkRowEncoderImpl extends SparkRowEncoder
