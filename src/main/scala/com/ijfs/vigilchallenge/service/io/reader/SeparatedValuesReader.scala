package com.ijfs.vigilchallenge.service.io.reader

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.encoder.SparkRowEncoderImpl
import com.ijfs.vigilchallenge.encoder.SparkRowEncoderImpl._
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.wrapper.spark.{SparkSessionWrapper, StructTypeWrapper}
import fs2.Stream
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.net.URI

trait SeparatedValuesReader[F[_]] {

  def readMultiple(url: URI): F[
    Either[
      VigilFailure,
      Stream[F, (String, String)]
    ]
  ]

}

object SeparatedValuesReader {
  def make[F[_] : Async](sparkSession: SparkSessionWrapper): SeparatedValuesReader[F] = new SeparatedValuesReaderImpl[F](
    sparkSession = sparkSession,
    dataSchema = new StructTypeWrapper(
      new StructType()
        .add(StructField("key", StringType, false))
        .add(StructField("value", StringType, false)),
    ),
    SparkRowEncoderImpl
  )
}
