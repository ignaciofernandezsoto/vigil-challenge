package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.hadoop.fs.Path

case class PathWrapper(path: Path) {

  private[spark] def get: Path = path

}

object PathWrapper {
  def apply(pathString: String) = new PathWrapper(new Path(pathString))
}