package com.ijfs.vigilchallenge.service

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.reader.SeparatedValuesReader
import com.ijfs.vigilchallenge.service.io.writer.MultipleTsvWriter
import com.ijfs.vigilchallenge.service.model.PipelineUrls
import com.ijfs.vigilchallenge.service.reducer.DataReducer
import com.ijfs.vigilchallenge.service.sanitizer.DataSanitizer
import com.ijfs.vigilchallenge.wrapper.spark.SparkSessionWrapper
import fs2.Stream
import org.apache.spark.sql.SparkSession

trait PipelineDataReducer[F[_]] {
  
  def reduce(urls: PipelineUrls): Stream[F, Either[VigilFailure, Unit]]

}

object PipelineDataReducer {
  def make[F[_] : Async](sparkSession: SparkSessionWrapper): PipelineDataReducer[F] = new PipelineDataReducerImpl[F](
    SeparatedValuesReader.make(sparkSession),
    DataSanitizer.make,
    DataReducer.make,
    MultipleTsvWriter.make(sparkSession),
  )
}