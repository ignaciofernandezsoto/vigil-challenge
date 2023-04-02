package com.ijfs.vigilchallenge.validation

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import com.ijfs.vigilchallenge.service.error.Failures.{InsufficientArguments, InvalidUrl}
import com.ijfs.vigilchallenge.service.model.PipelineUrls
import com.ijfs.vigilchallenge.validation.ArgumentsValidatorSpec.{RAW_INVALID_INPUT_URL, RAW_INVALID_OUTPUT_URL, RAW_VALID_INPUT_URL, RAW_VALID_OUTPUT_URL}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers

import java.net.URI

class ArgumentsValidatorSpec
  extends AsyncFeatureSpec
    with GivenWhenThen
    with Matchers {

  val validator: ArgumentsValidator.type = ArgumentsValidator

  Feature("validateRequest") {

    Scenario("On no arguments, an InsufficientArguments invalid result should be returned") {
      Given("no arguments")
      val args = List.empty[String]

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("an InsufficientArguments invalid result should be returned")
      result must be(Invalid(NonEmptyList.one(InsufficientArguments)))
    }

    Scenario("On only one argument, an InsufficientArguments invalid result should be returned") {
      Given("only one argument")
      val args = RAW_VALID_INPUT_URL :: Nil

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("an InsufficientArguments invalid result should be returned")
      result must be(Invalid(NonEmptyList.one(InsufficientArguments)))
    }

    Scenario("On two arguments where both arguments are an invalid url, an InvalidUrl invalid result should be returned") {
      Given("two arguments where both arguments are an invalid url")
      val args = RAW_INVALID_INPUT_URL :: RAW_INVALID_OUTPUT_URL :: Nil

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("an InvalidRequestArguments invalid result should be returned")
      result match {
        case Invalid(NonEmptyList(InvalidUrl(RAW_INVALID_INPUT_URL, ex1), InvalidUrl(RAW_INVALID_OUTPUT_URL, ex2) :: Nil)) =>
          ex1 mustBe a[IllegalArgumentException]
          ex2 mustBe a[IllegalArgumentException]
        case _ => fail
      }
    }

    Scenario("On two arguments where the first one is an invalid url, an InvalidUrl invalid result should be returned") {
      Given("two arguments where the first one is an invalid url")
      val args = RAW_INVALID_INPUT_URL :: RAW_VALID_OUTPUT_URL :: Nil

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("an InvalidRequestArguments invalid result should be returned")
      result match {
        case Invalid(NonEmptyList(InvalidUrl(RAW_INVALID_INPUT_URL, ex), _)) => ex mustBe a[IllegalArgumentException]
        case _ => fail
      }
    }

    Scenario("On two arguments where the second one is an invalid url, an InvalidUrl invalid result should be returned") {
      Given("two arguments where the first one is an invalid url")
      val args = RAW_VALID_INPUT_URL :: RAW_INVALID_OUTPUT_URL :: Nil

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("an InvalidRequestArguments invalid result should be returned")
      result match {
        case Invalid(NonEmptyList(InvalidUrl(RAW_INVALID_OUTPUT_URL, ex), _)) => ex mustBe a[IllegalArgumentException]
        case _ => fail
      }
    }

    Scenario("On two valid url arguments, a valid PipelineUrls should be returned") {
      Given("two arguments where the first one is an invalid url")
      val args = RAW_VALID_INPUT_URL :: RAW_VALID_OUTPUT_URL :: Nil

      When("validator is called")
      val result = validator.validateRequest(args)

      Then("a valid PipelineUrls should be returned")
      result must be(Valid(PipelineUrls(new URI(RAW_VALID_INPUT_URL), new URI(RAW_VALID_OUTPUT_URL))))
    }

  }

}

object ArgumentsValidatorSpec {

  private final val RAW_VALID_INPUT_URL = "s3a://url/input"
  private final val RAW_VALID_OUTPUT_URL = "s3a://url/output"

  private final val RAW_INVALID_INPUT_URL = "   {s3/url/input"
  private final val RAW_INVALID_OUTPUT_URL = " } }s-2output"

}
