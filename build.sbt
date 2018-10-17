name := "Streaming Capstone"

version := "0.1"

scalaVersion := "2.11.8"

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11",
  "org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11"
).map(_ % "2.3.2")

// FS -> kafka
libraryDependencies ++= Seq(

)

// spark -> cassandra
libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.11"
).map(_ % "2.3.2")

// circe for json parsing
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.10.0")

mainClass in assembly := Some("Main")
