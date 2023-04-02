package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.hadoop.fs.FileSystem

import java.net.URI

class FileSystemCreatorWrapper {

  def get(uri: URI, conf: ConfigurationWrapper) = new FileSystemWrapper(FileSystem.get(uri, conf.get))

}
