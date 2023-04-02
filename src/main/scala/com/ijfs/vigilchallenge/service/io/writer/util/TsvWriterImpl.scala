package com.ijfs.vigilchallenge.service.io.writer.util

import cats.data.EitherT
import cats.effect.kernel.Async
import cats.implicits._
import com.ijfs.vigilchallenge.encoder.SparkRowEncoder
import com.ijfs.vigilchallenge.service.error.Failures.{RenamingFileFailure, WritingDataFailure}
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.model.Tsv
import com.ijfs.vigilchallenge.wrapper.spark.{EncoderWrapper, FileSystemCreatorWrapper, PathWrapper, SparkSessionWrapper}

import java.net.URI
import scala.util.Try

class TsvWriterImpl[F[_] : Async](
                     sparkSession: SparkSessionWrapper,
                     sparkEncoder: SparkRowEncoder,
                     fileSystem: FileSystemCreatorWrapper
                   ) extends TsvWriter[F] {

  override def write(url: URI, content: (String, String)): F[Either[VigilFailure, Unit]] = (for {
    _ <- EitherT(save(content, url))
    renamingFile <- EitherT(updateToTsv(url))
  } yield renamingFile).value

  private def save(keyValue: (String, String), path: URI): F[Either[VigilFailure, Unit]] = Async[F].delay {
    Try {
      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder
      sparkSession.createDataset(List(keyValue))
        .rdd
        .coalesce(1)
        .map {
          case (k, v) => s"$k\t$v"
        }
        .saveAsTextFile(path.toString)
    }.toEither
      .left
      .map {
        case exception: RuntimeException => WritingDataFailure(exception)
      }
  }

  private def updateToTsv(filePath: URI): F[Either[VigilFailure, Unit]] = Async[F].delay {
    Try {
      val fs = fileSystem.get(filePath, sparkSession.sparkContext.hadoopConfiguration)
      fs.rename(PathWrapper(s"$filePath/part-00000"), PathWrapper(s"$filePath/part-00000.${Tsv.name}"))
    }.toEither match {
      case Right(true) => ().asRight
      case Right(false) => RenamingFileFailure(filePath, None).asLeft
      case Left(e: RuntimeException) => RenamingFileFailure(filePath, Some(e)).asLeft
    }
  }

}
