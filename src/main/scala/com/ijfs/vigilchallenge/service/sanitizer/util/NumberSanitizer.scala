package com.ijfs.vigilchallenge.service.sanitizer.util

trait NumberSanitizer {

  def sanitize(value: String): Int

}

object NumberSanitizer {
  def make: NumberSanitizer = new NumberSanitizerImpl
}