import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext, Time}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

case class LogRecord(time: Long, categoryId: Long, ip: String, action: String)
case class WrappedLogRecord(value: String)

object Config {
  val SPARK_BATCH_INTERVAL = Seconds(1)
  val SPARK_DSTREAM_REMEMBER_INTERVAL = Minutes(10)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "fraud-detector",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer]
    //    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
}

object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val streamingContext = new StreamingContext(spark.sparkContext, Config.SPARK_BATCH_INTERVAL)

  val topics = Array("clickstream-log")

  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext, PreferConsistent, Subscribe[String, String](topics, Config.kafkaParams)
  )
  stream
    // TODO tweak Kafka properties or write own FileSystem connector (no) to avoid so many conversions
    .map(record => decode[WrappedLogRecord](record.value))
    .filter(record => record.isRight)
    .map(record => decode[LogRecord](record.right.get.value))
    .filter(record => record.isRight)
    .map(record =>
      (record.right.get.ip, 1)
    )
    .reduceByKeyAndWindow((x: Int, y: Int) => {x + y}, Seconds(1))
    .print()

  streamingContext.remember(Config.SPARK_DSTREAM_REMEMBER_INTERVAL)

  streamingContext.start()
  streamingContext.awaitTermination()
}
