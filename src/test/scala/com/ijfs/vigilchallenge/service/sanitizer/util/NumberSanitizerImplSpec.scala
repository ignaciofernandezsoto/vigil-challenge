package com.ijfs.vigilchallenge.service.sanitizer.util

import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Prop, Properties}
import org.scalatest.matchers.must.Matchers

class NumberSanitizerImplSpec
  extends Properties("NumberSanitizerImpl")
    with Matchers {

  val numberSanitizer = new NumberSanitizerImpl

  property("validStringToInt") = forAll { (n: Int) =>
    val input = n.toString
    Prop(numberSanitizer.sanitize(input) == n)
  }

  property("nullOrEmptyStringToInt") = forAll(Gen.oneOf(Seq("", null))) { (input: String) =>
    Prop(numberSanitizer.sanitize(input) == 0)
  }

}
