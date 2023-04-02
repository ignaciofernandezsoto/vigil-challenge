package com.ijfs.vigilchallenge.validation

import cats.data.ValidatedNel
import cats.implicits._
import com.ijfs.vigilchallenge.service.error.{Failures, VigilFailure}
import com.ijfs.vigilchallenge.service.model.PipelineUrls

import java.net.URI
import scala.util.{Failure, Success, Try}

object ArgumentsValidator {

  def validateRequest(args: List[String]): ValidatedNel[VigilFailure, PipelineUrls] =
    args match {
      case rawInputUrl :: rawOutputUrl :: _ =>
        List(validateUrl(rawInputUrl), validateUrl(rawOutputUrl)).sequence.map {
          case inputUrl :: outputUrl :: _ => PipelineUrls(inputUrl, outputUrl)
        }
      case _ => Failures.InsufficientArguments.invalidNel
    }

  private def validateUrl(rawUrl: String): ValidatedNel[VigilFailure, URI] = {
    Try(URI.create(rawUrl)) match {
      case Failure(ex: RuntimeException) => Failures.InvalidUrl(rawUrl, ex).invalidNel
      case Failure(thr: Throwable) => throw thr
      case Success(url) => url.validNel
    }
  }

}
