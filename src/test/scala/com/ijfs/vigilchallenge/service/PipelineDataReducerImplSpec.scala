package com.ijfs.vigilchallenge.service

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import com.ijfs.vigilchallenge.service.PipelineDataReducerImplSpec.{inputUrl, outputUrl}
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.reader.SeparatedValuesReader
import com.ijfs.vigilchallenge.service.io.writer.MultipleTsvWriter
import com.ijfs.vigilchallenge.service.model.PipelineUrls
import com.ijfs.vigilchallenge.service.reducer.DataReducer
import com.ijfs.vigilchallenge.service.reducer.model.KeyValueData
import com.ijfs.vigilchallenge.service.sanitizer.DataSanitizer
import fs2.Stream
import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito.StubbingOps
import org.mockito.MockitoSugar.mock
import org.mockito.cats.IdiomaticMockitoCats.StubbingOpsCats
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen}
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers

import java.net.URI

class PipelineDataReducerImplSpec
  extends AsyncFeatureSpec
    with BeforeAndAfterEach
    with AsyncIOSpec
    with GivenWhenThen
    with Matchers {

  trait SeparatedValuesReaderMock extends SeparatedValuesReader[IO]

  trait DataReducerMock extends DataReducer[IO]

  trait MultipleTsvWriterMock extends MultipleTsvWriter[IO]

  var urls: PipelineUrls = _

  var reader: SeparatedValuesReader[IO] = _
  var sanitizer: DataSanitizer = _
  var dataReducer: DataReducer[IO] = _
  var writer: MultipleTsvWriter[IO] = _

  var pipelineDataReducer: PipelineDataReducerImpl[IO] = _

  override def beforeEach(): Unit = {
    urls = mock[PipelineUrls]
    urls.input returns inputUrl
    urls.output returns outputUrl

    reader = mock[SeparatedValuesReaderMock]
    sanitizer = mock[DataSanitizer]
    dataReducer = mock[DataReducerMock]
    writer = mock[MultipleTsvWriterMock]

    pipelineDataReducer = new PipelineDataReducerImpl[IO](
      reader,
      sanitizer,
      dataReducer,
      writer,
    )
  }

  Feature("reduce") {

    Scenario("on failure to read file, the failure should be returned") {
      Given("failure to read file")
      val failure = mock[VigilFailure]
      reader.readMultiple(inputUrl) returnsF failure.asLeft

      When("service is called")
      val res = pipelineDataReducer.reduce(urls)

      Then("the failure should be returned")
      res.take(1).compile.toVector.asserting(_ must be(Vector(failure.asLeft)))
    }

    Scenario("on success to read file but with failures on writing the file, the failures should be returned") {
      Given("failure to read file")
      val failure1 = mock[VigilFailure]
      val failure2 = mock[VigilFailure]
      val failure3 = mock[VigilFailure]
      val reducedStream = Stream(("0", "1"), ("2", "3"), ("4", "5")).covary[IO]
      val outputStream = Stream.emits(Seq(failure1.asLeft, failure2.asLeft, failure3.asLeft)).covary[IO]

      reader.readMultiple(inputUrl) returnsF Stream.emits(List(("0", "1"), ("2", "3"), ("4", "5"), ("0", "2"), ("0", "2"))).covary[IO].asRight
      sanitizer.sanitize(("0", "1")) returns ((0, 1))
      sanitizer.sanitize(("2", "3")) returns ((2, 3))
      sanitizer.sanitize(("4", "5")) returns ((4, 5))
      sanitizer.sanitize(("0", "2")) returns ((0, 2))
      dataReducer.reduce(ArgumentMatchersSugar.*) answers {
        e: Stream[IO, KeyValueData] =>
          e.compile.toVector.asserting(_ must be(Vector((0, 1), (2, 3), (4, 5))))
          reducedStream
      }
      writer.writeMultiple(outputUrl, reducedStream) returns outputStream

      When("service is called")
      val res = pipelineDataReducer.reduce(urls)

      Then("the failures should be returned")
      res.take(3).compile.toVector.asserting(_ must be(Vector(failure1.asLeft, failure2.asLeft, failure3.asLeft)))
    }

    Scenario("on success to read file and on writing the file, the successes should be returned") {
      Given("failure to read file")
      val reducedStream = Stream(("0", "1"), ("2", "3"), ("4", "5")).covary[IO]
      val outputStream = Stream.emits(Seq(().asRight, ().asRight, ().asRight)).covary[IO]

      reader.readMultiple(inputUrl) returnsF Stream.emits(List(("0", "1"), ("2", "3"), ("4", "5"), ("0", "2"), ("0", "2"))).covary[IO].asRight
      sanitizer.sanitize(("0", "1")) returns ((0, 1))
      sanitizer.sanitize(("2", "3")) returns ((2, 3))
      sanitizer.sanitize(("4", "5")) returns ((4, 5))
      sanitizer.sanitize(("0", "2")) returns ((0, 2))
      dataReducer.reduce(ArgumentMatchersSugar.*) answers {
        e: Stream[IO, KeyValueData] =>
          e.compile.toVector.asserting(_ must be(Vector((0, 1), (2, 3), (4, 5))))
          reducedStream
      }
      writer.writeMultiple(outputUrl, reducedStream) returns outputStream

      When("service is called")
      val res = pipelineDataReducer.reduce(urls)

      Then("the successes should be returned")
      res.take(3).compile.toVector.asserting(_ must be(Vector(().asRight, ().asRight, ().asRight)))
    }

  }

}

private object PipelineDataReducerImplSpec {
  val inputUrl = URI.create("/")
  val outputUrl = URI.create("/")
}