package com.ijfs.vigilchallenge.service.io.writer

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.writer.util.TsvWriter
import fs2.Stream

import java.net.URI

class MultipleTsvWriterImpl[F[_] : Async](tsvWriter: TsvWriter[F])
  extends MultipleTsvWriter[F] {

  def writeMultiple(
                     url: URI,
                     content: Stream[F, (String, String)]
                   ): Stream[F, Either[VigilFailure, Unit]] =
    content.zipWithIndex.map { case (kv, idx) => (kv, new URI(s"$url/$idx")) }.flatMap {
      case (keyValue, path) =>
        Stream.eval(tsvWriter.write(path, keyValue))
    }



}
