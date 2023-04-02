package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.sql.Encoder

class EncoderWrapper[T](encoder: Encoder[T]) {
  private[spark] def get: Encoder[T] = encoder
}
