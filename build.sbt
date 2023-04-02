ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "vigil-challenge"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2",
  "org.apache.hadoop" % "hadoop-common" % "3.3.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "org.typelevel" %% "cats-effect" % "3.4.8",
  "co.fs2" %% "fs2-core" % "3.6.1",
  "com.github.seratch" %% "awscala-s3" % "0.9.2",
  "org.typelevel" %% "log4cats-core" % "2.5.0",
  "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
) ++ Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.12" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.17.12" % Test,
  "org.mockito" %% "mockito-scala-cats" % "1.17.12" % Test,
  "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
)

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

dockerBaseImage := "openjdk:8-jdk"