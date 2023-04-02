package com.ijfs.vigilchallenge.service.writer

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import com.ijfs.vigilchallenge.service.error.VigilFailure
import com.ijfs.vigilchallenge.service.io.writer.MultipleTsvWriterImpl
import com.ijfs.vigilchallenge.service.io.writer.util.TsvWriter
import fs2.Stream
import org.mockito.IdiomaticMockito.StubbingOps
import org.mockito.MockitoSugar.mock
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen}

import java.net.URI

class MultipleTsvWriterImplSpec
  extends AsyncFeatureSpec
    with BeforeAndAfterEach
    with AsyncIOSpec
    with GivenWhenThen
    with Matchers {

  var tsvWriter: TsvWriter[IO] = _

  var multipleTsvWriter: MultipleTsvWriterImpl[IO] = _

  trait EffectfulTsvWriter extends TsvWriter[IO]

  override def beforeEach(): Unit = {
    tsvWriter = mock[EffectfulTsvWriter]

    multipleTsvWriter = new MultipleTsvWriterImpl[IO](tsvWriter)
  }

  Feature("writeMultiple") {

    Scenario("on empty content, an empty stream is returned") {
      Given("empty content")
      val uri = new URI("/path/")
      val content = Stream.empty.covary[IO]

      When("writer is called")
      val res = multipleTsvWriter.writeMultiple(uri, content)

      Then("an empty stream is returned")
      res.compile.toList.asserting(_.length must be(0))
    }

    Scenario("on a single element that fails writing, said failure is returned") {
      Given("a single element that fails writing")
      val e = mock[VigilFailure]
      val uri = new URI("/path")
      val content = Stream.emit("0", "1").covary[IO]

      tsvWriter.write(new URI("/path/0"), ("0", "1")) returns e.asLeft.pure[IO]

      When("writer is called")
      val res = multipleTsvWriter.writeMultiple(uri, content)

      Then("a single failure is returned")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(e.asLeft))
    }

    Scenario("on a single element that successfully writes, a right is returned") {
      Given("a single element that successfully writes")
      val uri = new URI("/path")
      val content = Stream.emit("0", "1").covary[IO]

      tsvWriter.write(new URI("/path/0"), ("0", "1")) returns ().asRight.pure[IO]

      When("writer is called")
      val res = multipleTsvWriter.writeMultiple(uri, content)

      Then("a single failure is returned")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(().asRight))
    }

    Scenario("on two elements where one fails and the other is successful on writing, " +
      "a left and a right are returned") {
      Given("two elements where one fails and the other is successful on writing")
      val e = mock[VigilFailure]
      val uri = new URI("/path")
      val content = Stream.emits(Seq(("0", "1"), ("1", "2"))).covary[IO]

      tsvWriter.write(new URI("/path/0"), ("0", "1")) returns e.asLeft.pure[IO]
      tsvWriter.write(new URI("/path/1"), ("1", "2")) returns ().asRight.pure[IO]

      When("writer is called")
      val res = multipleTsvWriter.writeMultiple(uri, content)

      Then("a single failure is returned")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(e.asLeft, ().asRight))
    }

  }


}
