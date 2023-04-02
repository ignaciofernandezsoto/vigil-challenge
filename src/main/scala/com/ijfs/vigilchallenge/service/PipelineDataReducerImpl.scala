package com.ijfs.vigilchallenge.service

import cats.data.EitherT
import cats.effect.kernel.Async
import cats.implicits._
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.reader.SeparatedValuesReader
import com.ijfs.vigilchallenge.service.io.writer.MultipleTsvWriter
import com.ijfs.vigilchallenge.service.model.PipelineUrls
import com.ijfs.vigilchallenge.service.reducer.DataReducer
import com.ijfs.vigilchallenge.service.sanitizer.DataSanitizer
import fs2.Stream

class PipelineDataReducerImpl[F[_] : Async](
                                             reader: SeparatedValuesReader[F],
                                             sanitizer: DataSanitizer,
                                             dataReducer: DataReducer[F],
                                             writer: MultipleTsvWriter[F],
                                           )
  extends PipelineDataReducer[F] {

  def reduce(urls: PipelineUrls): Stream[F, Either[VigilFailure, Unit]] = Stream.eval(
    (for {
    files <- EitherT(reader.readMultiple(urls.input))
    sanitizedData <- EitherT(files.map(sanitizer.sanitize).asRight[VigilFailure].pure[F])
    reducedData <- EitherT(dataReducer.reduce(sanitizedData).asRight[VigilFailure].pure[F])
    wroteData <- EitherT(writer.writeMultiple(urls.output, reducedData).asRight[VigilFailure].pure[F])
  } yield wroteData).value map {
    case Left(e) => Stream(e.asLeft)
    case Right(value) => value
  }
  ).flatten

}
