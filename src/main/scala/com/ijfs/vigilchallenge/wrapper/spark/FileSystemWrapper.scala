package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.hadoop.fs.FileSystem

class FileSystemWrapper(fileSystem: FileSystem) {

  def rename(var1: PathWrapper, var2: PathWrapper) = fileSystem.rename(var1.get, var2.get)

}
