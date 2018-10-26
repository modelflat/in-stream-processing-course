name := "Streaming Capstone"

version := "0.2"

scalaVersion := "2.11.8"

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11",
  "org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11"
) .map(_ % "2.3.2")
  //.map(_ % "provided")

// ignite
libraryDependencies ++= Seq(
  "org.apache.ignite" % "ignite-core",
  "org.apache.ignite" % "ignite-spark"
).map(_ % "2.6.0")

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

// kafka -> spark
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2" excludeAll excludeJpountz
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2" excludeAll excludeJpountz

// spark -> cassandra
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"

// circe for json parsing
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.10.0")

mainClass in assembly := Some("Main")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
