name := "delta-lake-quick-start"
organization := "com.github.fescalhao"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false

val sparkVersion = "3.1.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

val deltaLakeDependencies = Seq(
  "io.delta" %% "delta-core" % "1.0.0"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ deltaLakeDependencies ++ testDependencies