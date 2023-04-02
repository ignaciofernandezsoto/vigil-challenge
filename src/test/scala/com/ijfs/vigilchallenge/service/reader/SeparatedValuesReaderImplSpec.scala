package com.ijfs.vigilchallenge.service.reader

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import com.ijfs.vigilchallenge.encoder.SparkRowEncoder
import com.ijfs.vigilchallenge.service.error.Failures.ReadingDataFailure
import com.ijfs.vigilchallenge.service.io.reader.SeparatedValuesReaderImpl
import com.ijfs.vigilchallenge.wrapper.spark._
import org.apache.spark.sql.Row
import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito.{StubbingOps, ThrowSomethingOps, thrown}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen}

import java.net.URI

class SeparatedValuesReaderImplSpec
  extends AsyncFeatureSpec
    with BeforeAndAfterEach
    with AsyncIOSpec
    with GivenWhenThen
    with Matchers {

  var sparkSession: SparkSessionWrapper = _
  var dataSchema: StructTypeWrapper = _
  implicit var sparkEncoder: SparkRowEncoder = _

  var separatedValuesReader: SeparatedValuesReaderImpl[IO] = _

  class RowDataFrameWrapper extends DataFrameWrapper[Row](null)

  class StringTupleEncoderWrapper extends EncoderWrapper[(String, String)](null)

  class StringTupleDataFrameWrapper extends DataFrameWrapper[(String, String)](null)

  override def beforeEach(): Unit = {
    sparkSession = mock[SparkSessionWrapper]
    dataSchema = mock[StructTypeWrapper]
    sparkEncoder = mock[SparkRowEncoder]
    sparkEncoder.encoder returns mock[StringTupleEncoderWrapper]

    separatedValuesReader = new SeparatedValuesReaderImpl[IO](
      sparkSession,
      dataSchema,
      sparkEncoder
    )
  }

  Feature("readMultiple") {

    Scenario("on csv read exception, a ReadingDataFailure failure is returned") {
      Given("csv read failure")
      val ex = new RuntimeException("FAILURE")
      val dataFrameReader1 = mock[DataFrameReaderWrapper]
      val dataFrameReader2 = mock[DataFrameReaderWrapper]
      val dataFrameReader3 = mock[DataFrameReaderWrapper]
      val dataFrameReader4 = mock[DataFrameReaderWrapper]
      val rowDataFrame = mock[RowDataFrameWrapper]

      sparkSession.read returns dataFrameReader1
      dataFrameReader1.schema(dataSchema) returns dataFrameReader2
      dataFrameReader2.format("csv") returns dataFrameReader3
      dataFrameReader3.option("header", true) returns dataFrameReader4
      dataFrameReader4.load("/*.csv") returns rowDataFrame

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      ex willBe thrown by rowDataFrame.map(ArgumentMatchersSugar.*)

      When("reader is called")
      val res = separatedValuesReader.readMultiple(new URI("/path"))

      Then("said failure is returned")
      res.asserting(_ must be(ReadingDataFailure(ex).asLeft))
    }

    Scenario("on csv successful read and tsv read exception, a ReadingDataFailure failure is returned") {
      Given("csv successful read and tsv read exception")
      val dataFrameReader1 = mock[DataFrameReaderWrapper]
      val dataFrameReader5 = mock[DataFrameReaderWrapper]
      val dataFrameReader2 = mock[DataFrameReaderWrapper]
      val dataFrameReader3 = mock[DataFrameReaderWrapper]
      val dataFrameReader4 = mock[DataFrameReaderWrapper]
      val rowDataFrame1 = mock[RowDataFrameWrapper]
      val stringTupleDataFrame1 = mock[StringTupleDataFrameWrapper]

      when(sparkSession.read) thenReturn dataFrameReader1 andThen dataFrameReader5
      dataFrameReader1.schema(dataSchema) returns dataFrameReader2
      dataFrameReader2.format("csv") returns dataFrameReader3
      dataFrameReader3.option("header", true) returns dataFrameReader4
      dataFrameReader4.load("/*.csv") returns rowDataFrame1

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      rowDataFrame1.map(ArgumentMatchersSugar.*) returns stringTupleDataFrame1

      val ex = new RuntimeException("FAILURE")
      val dataFrameReader6 = mock[DataFrameReaderWrapper]
      val dataFrameReader7 = mock[DataFrameReaderWrapper]
      val dataFrameReader8 = mock[DataFrameReaderWrapper]
      val dataFrameReader9 = mock[DataFrameReaderWrapper]
      val rowDataFrame2 = mock[RowDataFrameWrapper]

      dataFrameReader5.schema(dataSchema) returns dataFrameReader6
      dataFrameReader6.format("csv") returns dataFrameReader7
      dataFrameReader7.option("header", true) returns dataFrameReader8
      dataFrameReader8.option("sep", "\t") returns dataFrameReader9
      dataFrameReader9.load("/*.tsv") returns rowDataFrame2

      ex willBe thrown by rowDataFrame2.map(ArgumentMatchersSugar.*)

      When("reader is called")
      val res = separatedValuesReader.readMultiple(new URI("/path"))

      Then("said failure is returned")
      res.asserting(_ must be(ReadingDataFailure(ex).asLeft))
    }

    Scenario("on csv and tsv successful read, the merge between files should be returned") {
      Given("csv and tsv successful read")
      val dataFrameReader1 = mock[DataFrameReaderWrapper]
      val dataFrameReader5 = mock[DataFrameReaderWrapper]
      val dataFrameReader2 = mock[DataFrameReaderWrapper]
      val dataFrameReader3 = mock[DataFrameReaderWrapper]
      val dataFrameReader4 = mock[DataFrameReaderWrapper]
      val rowDataFrameWrapper = mock[RowDataFrameWrapper]
      val csvFile = mock[StringTupleDataFrameWrapper]

      when(sparkSession.read) thenReturn dataFrameReader1 andThen dataFrameReader5
      dataFrameReader1.schema(dataSchema) returns dataFrameReader2
      dataFrameReader2.format("csv") returns dataFrameReader3
      dataFrameReader3.option("header", true) returns dataFrameReader4
      dataFrameReader4.load("/*.csv") returns rowDataFrameWrapper

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      rowDataFrameWrapper.map(ArgumentMatchersSugar.*) returns csvFile

      val dataFrameReader6 = mock[DataFrameReaderWrapper]
      val dataFrameReader7 = mock[DataFrameReaderWrapper]
      val dataFrameReader8 = mock[DataFrameReaderWrapper]
      val dataFrameReader9 = mock[DataFrameReaderWrapper]
      val rowDataFrame2 = mock[RowDataFrameWrapper]
      val tsvFile = mock[StringTupleDataFrameWrapper]

      dataFrameReader5.schema(dataSchema) returns dataFrameReader6
      dataFrameReader6.format("csv") returns dataFrameReader7
      dataFrameReader7.option("header", true) returns dataFrameReader8
      dataFrameReader8.option("sep", "\t") returns dataFrameReader9
      dataFrameReader9.load("/*.tsv") returns rowDataFrame2

      rowDataFrame2.map(ArgumentMatchersSugar.*) returns tsvFile

      val mergedFile = mock[StringTupleDataFrameWrapper]

      csvFile.union(tsvFile) returns mergedFile

      mergedFile.toLocalIterator() returns new java.util.LinkedList[(String, String)](java.util.Arrays.asList(("0", "1"), ("2", "3"))).iterator()

      When("reader is called")
      val res = separatedValuesReader.readMultiple(new URI("/path"))

      Then("the merge between files should be returned")
      res.flatMap { case Right(stream) => stream.compile.toVector }.asserting(vector => vector must be(Vector(("0", "1"), ("2", "3"))))
    }

  }

}