package com.ijfs.vigilchallenge.service.io.reader

import cats.data.EitherT
import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.encoder.SparkRowEncoder
import com.ijfs.vigilchallenge.service.error.Failures.ReadingDataFailure
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.model.{Csv, SeparatedValuesType, Tsv}
import com.ijfs.vigilchallenge.service.io.reader.SeparatedValuesReaderImpl.{CHUNK_SIZE, HEADER}
import com.ijfs.vigilchallenge.wrapper.spark.{DataFrameWrapper, EncoderWrapper, SparkSessionWrapper, StructTypeWrapper}
import fs2.Stream

import java.net.URI
import scala.jdk.CollectionConverters._
import scala.util.Try

class SeparatedValuesReaderImpl[F[_] : Async](
                                               sparkSession: SparkSessionWrapper,
                                               dataSchema: StructTypeWrapper,
                                               sparkEncoder: SparkRowEncoder
                                             )
  extends SeparatedValuesReader[F] {

  override def readMultiple(url: URI): F[Either[VigilFailure, Stream[F, (String, String)]]] =
    (
      for {
        csvFiles <- EitherT(read(url, Csv))
        tsvFiles <- EitherT(read(url, Tsv))
      } yield Stream.fromIterator.apply(
        csvFiles.union(tsvFiles).toLocalIterator().asScala,
        CHUNK_SIZE
      )
      )
      .value

  private def read(
                    url: URI,
                    fileType: SeparatedValuesType,
                  ): F[Either[VigilFailure, DataFrameWrapper[(String, String)]]] = Async[F].delay {
    Try {
      val loadQuery = sparkSession
        .read // TODO use readStream
        .schema(dataSchema)
        .format(Csv.name)
        .option(HEADER, value = true)

      val loadQueryWithFormat = if (fileType == Tsv) loadQuery.option("sep", "\t") else loadQuery

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      loadQueryWithFormat
        .load(url.resolve(s"*.${fileType.name}").toString)
        .map(row =>
          (
            row.getString(0),
            row.getString(1)
          )
        )
    }
      .toEither.left.map {
      case exception: RuntimeException => ReadingDataFailure(exception).asInstanceOf[VigilFailure]
      case t@_ => throw t
    }
  }

}

object SeparatedValuesReaderImpl {

  private final val HEADER = "header"
  private final val CHUNK_SIZE = 4096

}
