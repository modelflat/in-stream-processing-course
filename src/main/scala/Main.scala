import io.circe.generic.auto._
import io.circe.parser._
import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

case class LogRecord(@transient raw: String, time: Long, categoryId: Long, ip: String, action: String)

case class Action(time: Long, categoryId: String, action: String)

case class IpStat(clicks: Long, views: Long, categories: Long)

object Config {
  val SPARK_BATCH_INTERVAL = Seconds(1)

  val SPARK_DSTREAM_REMEMBER_INTERVAL = Minutes(10) // OOM source :) // TODO fix?

  val TOPICS = Array("clickstream-log")

  val KAFKA_PARAMS = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
    "group.id" -> "fraud-detector",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "enable.auto.commit" -> "false"
  )
}

class IgniteState {



}

object Main extends App {
  val (spark, streamingContext) = makeSparkStuff()

//  val ignite = new IgniteContext(spark.sparkContext)

  val stream = makeFilteredAndParsedLogStream(streamingContext)

  stream.print()

  streamingContext.remember(Config.SPARK_DSTREAM_REMEMBER_INTERVAL)
  streamingContext.start()
  streamingContext.awaitTermination()

  def makeSparkStuff(): (SparkSession, StreamingContext) = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(spark.sparkContext, Config.SPARK_BATCH_INTERVAL)
    (spark, streamingContext)
  }

  def makeFilteredAndParsedLogStream(streamingContext: StreamingContext): DStream[(String, Action)] = {
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext, PreferConsistent, Subscribe[String, String](Config.TOPICS, Config.KAFKA_PARAMS)
    )

    stream
      .map(r => (r.key(), decode[Action](r.value())))
      .filter(entry => entry._1 != null && !entry._1.isEmpty && entry._2.isRight)
      .map(entry => (entry._1, entry._2.right.get))
  }

}
