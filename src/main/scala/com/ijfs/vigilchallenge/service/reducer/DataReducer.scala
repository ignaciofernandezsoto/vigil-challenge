package com.ijfs.vigilchallenge.service.reducer

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.service.reducer.model.KeyValueData
import fs2.Stream

trait DataReducer[F[_]] {

  def reduce(input: Stream[F, KeyValueData]): Stream[F, (String, String)]

}

object DataReducer {
  def make[F[_] : Async]: DataReducer[F] = new DataReducerImpl[F]
}
