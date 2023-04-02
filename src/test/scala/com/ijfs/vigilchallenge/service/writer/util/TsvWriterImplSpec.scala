package com.ijfs.vigilchallenge.service.writer.util

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.ijfs.vigilchallenge.encoder.SparkRowEncoder
import com.ijfs.vigilchallenge.service.error.Failures.{RenamingFileFailure, WritingDataFailure}
import com.ijfs.vigilchallenge.service.io.writer.MultipleTsvWriterImpl
import com.ijfs.vigilchallenge.service.io.writer.util.TsvWriterImpl
import com.ijfs.vigilchallenge.wrapper.spark.{ConfigurationWrapper, DataFrameWrapper, EncoderWrapper, FileSystemCreatorWrapper, FileSystemWrapper, PathWrapper, RDDWrapper, SparkContextWrapper, SparkSessionWrapper}
import fs2.Stream
import org.apache.spark.sql.Row
import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito.{StubbingOps, ThrowSomethingOps, thrown}
import org.mockito.MockitoSugar.{doNothing, mock}
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen}

import java.net.URI

class TsvWriterImplSpec
  extends AsyncFeatureSpec
    with BeforeAndAfterEach
    with AsyncIOSpec
    with GivenWhenThen
    with Matchers {

  var sparkSession: SparkSessionWrapper = _
  var sparkEncoder: SparkRowEncoder = _
  var fileSystemCreator: FileSystemCreatorWrapper = mock[FileSystemCreatorWrapper]

  var tsvWriter: TsvWriterImpl[IO] = _

  class StringTupleEncoderWrapper extends EncoderWrapper[(String, String)](null)
  class StringTupleDataFrameWrapper extends DataFrameWrapper[(String, String)](null)
  class StringTupleRDDWrapper extends RDDWrapper[(String, String)](null)
  class StringRDDWrapper extends RDDWrapper[String](null)


  override def beforeEach(): Unit = {
    sparkSession = mock[SparkSessionWrapper]
    sparkEncoder = mock[SparkRowEncoder]
    fileSystemCreator = mock[FileSystemCreatorWrapper]
    sparkEncoder.encoder returns mock[StringTupleEncoderWrapper]

    tsvWriter = new TsvWriterImpl[IO](
      sparkSession,
      sparkEncoder,
      fileSystemCreator
    )
  }

  Feature("write") {

    Scenario("on failure while writing, a left WritingDataFailure is returned") {
      Given("failure while writing")
      val ex = new RuntimeException("FAILURE")
      val uri = new URI("/path/")
      val content = ("0", "1")

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      val dataFrame = mock[StringTupleDataFrameWrapper]
      val rdd1 = mock[StringTupleRDDWrapper]
      val rdd2 = mock[StringTupleRDDWrapper]

      sparkSession.createDataset(List(content)) returns dataFrame
      dataFrame.rdd returns rdd1
      rdd1.coalesce(1) returns rdd2
      ex willBe thrown by rdd2.map[String](ArgumentMatchersSugar.*)

      When("writer is called")
      val res = tsvWriter.write(uri, content)

      Then("a left WritingDataFailure is returned")
      res.asserting(_ must be(Left(WritingDataFailure(ex))))
    }

    Scenario("on failure while updating file name, a left RenamingFileFailure is returned") {
      Given("failure while updating file name")
      val ex = new RuntimeException("FAILURE")
      val uri = new URI("/path")
      val content = ("0", "1")

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      val dataFrame = mock[StringTupleDataFrameWrapper]
      val rdd1 = mock[StringTupleRDDWrapper]
      val rdd2 = mock[StringTupleRDDWrapper]
      val rdd3 = mock[StringRDDWrapper]

      sparkSession.createDataset(List(content)) returns dataFrame
      dataFrame.rdd returns rdd1
      rdd1.coalesce(1) returns rdd2
      rdd2.map[String](ArgumentMatchersSugar.*) returns rdd3
      rdd3.saveAsTextFile("/path").doesNothing()

      val fileSystem = mock[FileSystemWrapper]
      val sparkContext = mock[SparkContextWrapper]
      val hadoopConfiguration = mock[ConfigurationWrapper]

      sparkSession.sparkContext returns sparkContext
      sparkContext.hadoopConfiguration returns hadoopConfiguration

      fileSystemCreator.get(uri, hadoopConfiguration) returns fileSystem
      ex willBe thrown by fileSystem.rename(PathWrapper(s"/path/part-00000"), PathWrapper(s"/path/part-00000.tsv"))

      When("writer is called")
      val res = tsvWriter.write(uri, content)

      Then("a left RenamingFileFailure is returned")
      res.asserting(_ must be(Left(RenamingFileFailure(uri, Some(ex)))))
    }

    Scenario("on unsuccessful while updating file name, a left RenamingFileFailure is returned") {
      Given("unsuccessful while updating file name")
      val uri = new URI("/path")
      val content = ("0", "1")

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      val dataFrame = mock[StringTupleDataFrameWrapper]
      val rdd1 = mock[StringTupleRDDWrapper]
      val rdd2 = mock[StringTupleRDDWrapper]
      val rdd3 = mock[StringRDDWrapper]

      sparkSession.createDataset(List(content)) returns dataFrame
      dataFrame.rdd returns rdd1
      rdd1.coalesce(1) returns rdd2
      rdd2.map[String](ArgumentMatchersSugar.*) returns rdd3
      rdd3.saveAsTextFile("/path").doesNothing()

      val fileSystem = mock[FileSystemWrapper]
      val sparkContext = mock[SparkContextWrapper]
      val hadoopConfiguration = mock[ConfigurationWrapper]

      sparkSession.sparkContext returns sparkContext
      sparkContext.hadoopConfiguration returns hadoopConfiguration

      fileSystemCreator.get(uri, hadoopConfiguration) returns fileSystem
      fileSystem.rename(PathWrapper(s"/path/part-00000"), PathWrapper(s"/path/part-00000.tsv")) returns false

      When("writer is called")
      val res = tsvWriter.write(uri, content)

      Then("a left RenamingFileFailure is returned")
      res.asserting(_ must be(Left(RenamingFileFailure(uri, None))))
    }

    Scenario("on successful saving and updating file, a right is returned") {
      Given("successful saving and updating file")
      val uri = new URI("/path")
      val content = ("0", "1")

      implicit val encoder: EncoderWrapper[(String, String)] = sparkEncoder.encoder

      val dataFrame = mock[StringTupleDataFrameWrapper]
      val rdd1 = mock[StringTupleRDDWrapper]
      val rdd2 = mock[StringTupleRDDWrapper]
      val rdd3 = mock[StringRDDWrapper]

      sparkSession.createDataset(List(content)) returns dataFrame
      dataFrame.rdd returns rdd1
      rdd1.coalesce(1) returns rdd2
      rdd2.map[String](ArgumentMatchersSugar.*) returns rdd3
      rdd3.saveAsTextFile("/path").doesNothing()

      val fileSystem = mock[FileSystemWrapper]
      val sparkContext = mock[SparkContextWrapper]
      val hadoopConfiguration = mock[ConfigurationWrapper]

      sparkSession.sparkContext returns sparkContext
      sparkContext.hadoopConfiguration returns hadoopConfiguration

      fileSystemCreator.get(uri, hadoopConfiguration) returns fileSystem
      fileSystem.rename(PathWrapper(s"/path/part-00000"), PathWrapper(s"/path/part-00000.tsv")) returns true

      When("writer is called")
      val res = tsvWriter.write(uri, content)

      Then("a right is returned")
      res.asserting(_ must be(Right(())))
    }

  }

}
