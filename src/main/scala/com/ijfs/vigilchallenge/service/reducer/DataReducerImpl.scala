package com.ijfs.vigilchallenge.service.reducer

import cats.effect.kernel.Async
import com.ijfs.vigilchallenge.service.reducer.model.{Key, KeyValueData, Value}
import fs2.Stream

class DataReducerImpl[F[_] : Async] extends DataReducer[F] {

  override def reduce(input: Stream[F, KeyValueData]): Stream[F, (String, String)] =
    input
      .chunks
      .map(
        _.foldLeft(Map.empty[Key, Map[Value, Int]]) {
          case (groupedValues, (key, value)) =>
            groupedValues.get(key) match {
              case Some(existingValues) =>
                groupedValues + (key -> (existingValues.get(value) match {
                  case Some(existingValueAmount) => existingValues + (value -> (existingValueAmount + 1))
                  case None => existingValues + (value -> 1)
                }))
              case None =>
                groupedValues + (key -> Map(value -> 1))
            }
        }
      )
      .fold(Map.empty[Key, Map[Value, Int]]) {
        case (accum, grouped) =>
          (accum.keySet ++ grouped.keySet).foldLeft(Map.empty[Key, Map[Value, Int]]) { (acc, k) =>
            val v1 = accum.getOrElse(k, Map.empty[Value, Int])
            val v2 = grouped.getOrElse(k, Map.empty[Value, Int])
            acc.updated(k, (v1.keySet ++ v2.keySet).foldLeft(Map.empty[Value, Int]) { (innerAcc, innerK) =>
              innerAcc.updated(innerK, v1.getOrElse(innerK, 0) + v2.getOrElse(innerK, 0))
            })
          }
      }
      .map(_.map {
        case (key, valuesOccurrences) => (
          key.toString,
          // "For any given key across the entire dataset (so all the files),
          // there exists exactly one value that occurs an odd number of times"
          valuesOccurrences.find(_._2 % 2 != 0).get._1.toString
        )
      })
      .flatMap(e => Stream.emits(e.toList))

}
