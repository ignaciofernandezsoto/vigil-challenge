package com.ijfs.vigilchallenge.service.sanitizer

import com.ijfs.vigilchallenge.service.reducer.model.{Key, Value}
import com.ijfs.vigilchallenge.service.sanitizer.util.NumberSanitizer

class DataSanitizerImpl(numberSanitizer: NumberSanitizer) extends DataSanitizer {

  def sanitize(keyValue: (String, String)): (Key, Value) = keyValue match {
    case (key, value) => (
      numberSanitizer.sanitize(key),
      numberSanitizer.sanitize(value)
    )
  }

}
