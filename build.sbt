import sbt.Package._
import sbt._
import Docker.autoImport.exposedPorts

scalaVersion := "2.12.4"

enablePlugins(DockerPlugin)
exposedPorts := Seq(8666)

libraryDependencies ++= Vector (
  Library.vertx_lang_scala,
  Library.vertx_web,
  Library.scalaTest       % "test",
  Library.vertx_kafka_client,
  Library.vertx_config,
  Library.logback,
  Library.scala_logging,
  // Uncomment for clustering
  // Library.vertx_hazelcast,

  //required to get rid of some warnings emitted by the scala-compile
  Library.vertx_codegen
)

packageOptions += ManifestAttributes(
  ("Main-Verticle", "scala:com.unisys.os2200.HttpVerticle"))

