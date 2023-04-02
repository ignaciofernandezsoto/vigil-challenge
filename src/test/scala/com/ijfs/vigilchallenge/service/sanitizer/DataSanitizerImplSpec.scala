package com.ijfs.vigilchallenge.service.sanitizer

import com.ijfs.vigilchallenge.service.reducer.model.{Key, Value}
import com.ijfs.vigilchallenge.service.sanitizer.util.NumberSanitizer
import org.mockito.IdiomaticMockito.StubbingOps
import org.mockito.MockitoSugar.mock
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers

class DataSanitizerImplSpec
  extends AsyncFeatureSpec
    with GivenWhenThen
    with Matchers {

  val numberSanitizer = mock[NumberSanitizer]
  val dataSanitizer = new DataSanitizerImpl(numberSanitizer)

  Feature("sanitize") {

    Scenario("on two strings, the resulting values from the numberSanitizer are returned") {
      Given("two strings")
      numberSanitizer.sanitize("22") returns 22
      numberSanitizer.sanitize(null) returns 0

      When("the sanitizer is called")
      val res = dataSanitizer.sanitize(("22", null))

      Then("the resulting values from the numberSanitizer are returned")
      res must be((22, 0))

    }

  }

}
