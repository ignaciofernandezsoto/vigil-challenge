package com.ijfs.vigilchallenge.service.reducer

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.must.Matchers

class DataReducerImplSpec
  extends AsyncFeatureSpec
    with AsyncIOSpec
    with GivenWhenThen
    with Matchers {

  val dataReducer = new DataReducerImpl[IO]()

  Feature("reduce") {

    Scenario("an empty stream should return an empty stream") {
      Given("empty stream")
      val stream = Stream.empty.covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns an empty stream")
      res.compile.toList.asserting(_.isEmpty must be(true))
    }

    Scenario("a stream of non matching keys should return the same stream") {
      Given("stream of non matching keys")
      val stream = Stream.emits(Seq((0, 1), (1, 2), (2, 3))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns the same stream")
      res.compile.toList.asserting(_ must contain theSameElementsAs (List(("0", "1"), ("1", "2"), ("2", "3"))))
    }

    Scenario("a stream of non matching keys separated in multiple chunks should return the same stream") {
      Given("stream of non matching keys separated in multiple chunks")
      val stream =
        Stream.emits(Seq((0, 1), (1, 2), (2, 3))).covary[IO] ++
          Stream.emits(Seq((3, 4), (4, 5), (5, 6))).covary[IO] ++
          Stream.emits(Seq((6, 7), (7, 8), (8, 9))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns the same stream")
      res.compile.toList.asserting(
        _ must contain theSameElementsAs List(
          ("0", "1"), ("1", "2"), ("2", "3"),
          ("3", "4"), ("4", "5"), ("5", "6"),
          ("6", "7"), ("7", "8"), ("8", "9"),
        )
      )
    }

    Scenario("a stream of only matching keys an odd amount of times " +
      "should return a stream with a single key and its value") {
      Given("stream of only matching keys an odd amount of times")
      val stream = Stream.emits(Seq((0, 1), (0, 1), (0, 1))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns a stream with a single key and the count of all matches")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(("0", "1")))
    }

    Scenario("a stream of only matching keys with one value existing an odd amount of times and another one an even amount of times, " +
      "should return a stream with a single key and its value that happened an odd amount of times") {
      Given("stream of only matching keys with one value existing an odd amount of times and another one an even amount of times")
      val stream = Stream.emits(Seq((0, 1), (0, 1), (0, 1), (0, 2), (0, 2))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns a stream with a single key and its value that happened an odd amount of times")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(("0", "1")))
    }

    Scenario("a stream separated into multiple chunks of only matching keys an odd amount of times " +
      "should return a stream with a single key and its value") {
      Given("stream separated into multiple chunks of only matching keys an odd amount of times")
      val stream = Stream.emits(Seq((0, 1), (0, 1))).covary[IO] ++
        Stream.emits(Seq((0, 1), (0, 1))).covary[IO] ++
        Stream.emits(Seq((0, 1), (0, 1))).covary[IO] ++
        Stream.emits(Seq((0, 1))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns a stream with a single key and the count of all matches")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(("0", "1")))
    }

    Scenario(
      "a stream separated into multiple chunks of only matching keys with one value existing " +
      "an odd amount of times and another one an even amount of times, " +
      "should return a stream with a single key and its value that happened an odd amount of times"
    ) {
      Given("stream separated into multiple chunks of only matching keys with one value existing " +
        "an odd amount of times and another one an even amount of times")
      val stream = Stream.emits(Seq((0, 1), (0, 2), (0, 1), (0, 1))).covary[IO] ++
        Stream.emits(Seq((0, 1), (0, 2), (0, 1), (0, 2))).covary[IO] ++
        Stream.emits(Seq((0, 1))).covary[IO] ++
        Stream.emits(Seq((0, 1), (0, 2))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns a stream with a single key and its value that happened an odd amount of times")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(("0", "1")))
    }

    Scenario(
      "a stream separated into multiple chunks of the following key values " +
        "[(1, 2), (2, 4), (1, 3), (2, 4), (3, 3), (1, 2), (2, 4)], " +
        "[(2, 4), (1, 2), (3, 0), (1, 3), (3, 3), (0, 2), (1, 3), (0, 2), (2, 5), (0, 1), (1, 3)] and " +
        "[(0, 1), (0, 2)] should return [(0, 2), (1, 2), (2, 5), (3, 0)]"
    ) {
      Given("stream separated into multiple chunks of only matching keys with one value being" +
        "an odd amount of times and another one an even amount of times")
      val stream = Stream.emits(Seq((1, 2), (2, 4), (1, 3), (2, 4), (3, 3), (1, 2), (2, 4))).covary[IO] ++
        Stream.emits(Seq((2, 4), (1, 2), (3, 0), (1, 3), (3, 3), (0, 2), (1, 3), (0, 2), (2, 5), (0, 1), (1, 3))).covary[IO] ++
        Stream.emits(Seq((0, 1), (0, 2))).covary[IO]

      When("reducer is called")
      val res = dataReducer.reduce(stream)

      Then("returns a stream with a single key and its value that happened an odd amount of times")
      res.compile.toList.asserting(_ must contain theSameElementsAs List(("0", "2"), ("1", "2"), ("2", "5"), ("3", "0")))
    }

  }
}
