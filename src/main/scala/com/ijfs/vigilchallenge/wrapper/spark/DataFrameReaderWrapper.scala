package com.ijfs.vigilchallenge.wrapper.spark

import org.apache.spark.sql.{DataFrameReader, Row}

class DataFrameReaderWrapper(dataFrameReader: DataFrameReader) {

  def schema(schema: StructTypeWrapper): DataFrameReaderWrapper =
    new DataFrameReaderWrapper(dataFrameReader.schema(schema.get))

  def format(source: String): DataFrameReaderWrapper =
    new DataFrameReaderWrapper(dataFrameReader.format(source))

  def option(key: String, value: Boolean): DataFrameReaderWrapper =
    new DataFrameReaderWrapper(dataFrameReader.option(key, value))

  def option(key: String, value: String): DataFrameReaderWrapper =
    new DataFrameReaderWrapper(dataFrameReader.option(key, value))

  def load(path: String): DataFrameWrapper[Row] =
    new DataFrameWrapper(dataFrameReader.load(path))

}
