package com.ijfs.vigilchallenge.service.io.model

abstract class SeparatedValuesType(val name: String)

case object Csv extends SeparatedValuesType("csv")

case object Tsv extends SeparatedValuesType("tsv")


