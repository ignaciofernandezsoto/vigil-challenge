package com.ijfs.vigilchallenge.service.sanitizer

import com.ijfs.vigilchallenge.service.reducer.model.{Key, Value}
import com.ijfs.vigilchallenge.service.sanitizer.util.NumberSanitizer

trait DataSanitizer {
  def sanitize(keyValue: (String, String)): (Key, Value)
}

object DataSanitizer {
  def make: DataSanitizer = new DataSanitizerImpl(NumberSanitizer.make)
}