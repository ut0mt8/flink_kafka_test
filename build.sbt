ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "test_flink"

version := "0.1"

organization := "com.futomaki"

ThisBuild / scalaVersion := "2.12.10"

val flinkVersion = "1.10.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

