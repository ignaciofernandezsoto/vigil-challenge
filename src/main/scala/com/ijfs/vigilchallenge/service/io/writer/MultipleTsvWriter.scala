package com.ijfs.vigilchallenge.service.io.writer

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.encoder.SparkRowEncoderImpl
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.writer.util.TsvWriter
import com.ijfs.vigilchallenge.wrapper.spark.{FileSystemCreatorWrapper, SparkSessionWrapper}
import fs2.Stream

import java.net.URI

trait MultipleTsvWriter[F[_]] {

  def writeMultiple(
             url: URI,
             content: Stream[F, (String, String)]
           ): Stream[F, Either[VigilFailure, Unit]]

}

object MultipleTsvWriter {
  def make[F[_] : Async](sparkSession: SparkSessionWrapper): MultipleTsvWriter[F] =
    new MultipleTsvWriterImpl[F](TsvWriter.make(sparkSession))
}
