package com.ijfs.vigilchallenge.service.sanitizer.util

class NumberSanitizerImpl extends NumberSanitizer {

  override def sanitize(rawNumber: String): Int = Option(rawNumber) match {
    case None => 0
    case Some(value) if value.isEmpty => 0
    case Some(value) => value.toInt
  }

}
