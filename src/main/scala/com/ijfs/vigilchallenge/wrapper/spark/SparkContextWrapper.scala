package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.SparkContext

class SparkContextWrapper(sparkContext: SparkContext) {

  def hadoopConfiguration: ConfigurationWrapper = new ConfigurationWrapper(sparkContext.hadoopConfiguration)

}
