package com.ijfs.vigilchallenge

import cats.data.Validated.{Invalid, Valid}
import cats.effect.kernel.Sync
import cats.effect.{ExitCode, IO, IOApp}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.ijfs.vigilchallenge.service.PipelineDataReducer
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.validation.ArgumentsValidator.validateRequest
import com.ijfs.vigilchallenge.wrapper.spark.SparkSessionWrapper
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp {

  implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def run(args: List[String]): IO[ExitCode] = {

    validateRequest(args) match {
      case Valid(request) =>
        for {
          spark <- createSparkSession
          _ <- injectCredentials(spark)
          wrappedSpark = new SparkSessionWrapper(spark)
          pipelineDataReducer = PipelineDataReducer.make[IO](wrappedSpark)
          reducedData <- pipelineDataReducer.reduce(request).compile.toList
          (successAmount, failures) = reducedData.foldLeft((0, List.empty[VigilFailure])) {
            case ((successAmount, failures), Left(e)) => (successAmount, e :: failures)
            case ((successAmount, failures), Right(_)) => (successAmount + 1, failures)
          }
          result <- (successAmount, failures) match {
            case (successAmount, Nil) =>
              logger[IO].info(s"Success! $successAmount files were successfully created")
                .as(ExitCode.Success)
            case (successAmount, failures) =>
              logger[IO].error(s"Oops! $successAmount files were successfully created and the following ${failures.size} errors occurred: $failures")
                .as(ExitCode.Error)

          }
        } yield result
      case Invalid(e) =>
        logger[IO].error(s"Error while validating request with error=[$e]")
          .as(ExitCode.Error)
    }

  }

  private def injectCredentials(spark: SparkSession): IO[Unit] = Sync[IO].delay {
    val sc = spark.sparkContext

    val credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials

    sc.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
    sc.hadoopConfiguration.set("fs.s3a.impl", classOf[S3AFileSystem].getName)
  }

  private def createSparkSession: IO[SparkSession] = Sync[IO].delay {
    SparkSession
      .builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
  }

}
