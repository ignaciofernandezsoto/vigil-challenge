package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.sql.types.StructType

class StructTypeWrapper(structType: StructType) {

  private[spark] def get: StructType = structType

}
