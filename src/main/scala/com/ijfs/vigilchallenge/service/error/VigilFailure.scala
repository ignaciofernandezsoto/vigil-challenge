package com.ijfs.vigilchallenge.service.error

import java.net.URI

object Failures {
  case object InsufficientArguments extends VigilFailure("Two S3A urls should be passed as arguments to the application. The first argument is the input, and the second argument the output")
  case class InvalidUrl(url: String, ex: RuntimeException) extends VigilFailure(s"The given url is invalid: [$url]", Some(ex))
  case class ReadingDataFailure(ex: RuntimeException) extends VigilFailure("Error while reading data", Some(ex))
  case class WritingDataFailure(ex: RuntimeException) extends VigilFailure("Error while writing data", Some(ex))
  case class RenamingFileFailure(path: URI, e: Option[RuntimeException]) extends VigilFailure(s"Error while renaming file. File=[$path] Error=[$e]")
}

abstract class VigilFailure(
                           val message: String,
                           val error: Option[RuntimeException] = None
                         )