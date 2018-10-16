name := "Streaming Capstone"

version := "0.1"

scalaVersion := "2.11.8"

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.2",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.2",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.2",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.2"
)

// FS -> kafka
libraryDependencies ++= Seq(

)

// spark -> cassandra
libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.3.2"
)


mainClass in assembly := Some("Main")
