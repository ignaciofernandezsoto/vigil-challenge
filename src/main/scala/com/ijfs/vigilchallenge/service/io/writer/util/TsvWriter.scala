package com.ijfs.vigilchallenge.service.io.writer.util

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.encoder.SparkRowEncoderImpl
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.wrapper.spark.{FileSystemCreatorWrapper, SparkSessionWrapper}

import java.net.URI

trait TsvWriter[F[_]] {
  def write(
             url: URI,
             content: (String, String)
           ): F[Either[VigilFailure, Unit]]
}

object TsvWriter {
  def make[F[_] : Async](sparkSession: SparkSessionWrapper): TsvWriter[F] = new TsvWriterImpl[F](
    sparkSession, SparkRowEncoderImpl, new FileSystemCreatorWrapper
  )
}