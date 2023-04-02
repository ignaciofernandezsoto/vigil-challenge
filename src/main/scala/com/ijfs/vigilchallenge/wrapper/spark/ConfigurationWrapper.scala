package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.hadoop.conf.Configuration

class ConfigurationWrapper(configuration: Configuration) {
  private[spark] def get: Configuration = configuration
}
