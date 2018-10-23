import java.time.Instant
import java.util.Calendar

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.kafka010._

import scala.concurrent.duration._

object Config {
  val BOT_IP_CASSANDRA_TTL = 1.minutes
  val WATERMARK = 30
  val BOT_CATEGORY_LIMIT = 10
  val BOT_REQUEST_LIMIT = 1000
  val BOT_CLICKS_TO_VIEWS_LIMIT = 5
  val BOT_CLICKS_TO_VIEWS_MIN_FRAMES = 5
  val DETECTION_WINDOW_INTERVAL = Seconds(15)
  val DETECTION_SLIDE_INTERVAL = DETECTION_WINDOW_INTERVAL
  val SPARK_BATCH_INTERVAL = Seconds(5)
  val SPARK_DSTREAM_REMEMBER_INTERVAL = Minutes(10)

  val TOPICS = Array("clickstream-log")

  val KAFKA_PARAMS = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
    "group.id" -> "fraud-detector",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer]
  )
}

case class LogRecord(@transient raw: String, time: Long, categoryId: Long, ip: String, action: String)
case class IpStat(clicks: Long, views: Long, categories: Set[String]) {
  def +(other: IpStat): IpStat = {
    IpStat(clicks + other.clicks, views + other.views, categories ++ other.categories)
  }
}

object IpStat {
  def empty(): IpStat = {
    IpStat(0, 0, Set.empty[String])
  }
}

case class Action(time: Long, categoryId: String, action: String) {
  def toIpStat: IpStat = {
    IpStat(
      if (action == "click") 1 else 0, if (action == "view") 1 else 0,
      Set(categoryId)
    )
  }
}
case class EvaluatedStat(originalStat: IpStat, isBot: Boolean, reason: String)

object EvaluatedStat {
  def classify(ipStat: List[IpStat]): EvaluatedStat = {
    val aggr = ipStat reduce (_+_)

    val strangeClicksToViews =
      if (ipStat.size >= Config.BOT_CLICKS_TO_VIEWS_MIN_FRAMES || aggr.views > 0)
        (aggr.clicks.toDouble / math.max(aggr.views, 1)) > Config.BOT_CLICKS_TO_VIEWS_LIMIT
      else false

    val tooManyRequests = (aggr.clicks + aggr.views) > Config.BOT_REQUEST_LIMIT
    val tooManyCategories = aggr.categories.size > Config.BOT_CATEGORY_LIMIT

    EvaluatedStat(aggr, tooManyRequests || tooManyCategories || strangeClicksToViews,
      if (tooManyRequests) "requests" else
      if (tooManyCategories) "categories" else
      if (strangeClicksToViews) "clicks/views" else
      "clear"
    )
  }
}

object Main extends App {
  val (spark, streamingContext) = makeSparkStuff()
  val stream = makeFilteredAndParsedLogStream(streamingContext)

  streamingContext.checkpoint("/tmp/spark-checkpoint")

  val botStream = findBots(stream)

  botStream.print()

  toCassandra(
    spark.sparkContext,
    botStream.map(ipAndStat => ipAndStat._1)
  )

  streamingContext.remember(Config.SPARK_DSTREAM_REMEMBER_INTERVAL)
  streamingContext.start()
  streamingContext.awaitTermination()

  def makeSparkStuff(): (SparkSession, StreamingContext) = {
    val spark = SparkSession.builder.master("local[*]")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.cassandra.connection.keep_alive_ms", 120000)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(spark.sparkContext, Config.SPARK_BATCH_INTERVAL)
    (spark, streamingContext)
  }

  def makeFilteredAndParsedLogStream(streamingContext: StreamingContext): DStream[((String, Long), IpStat)] = {
    val stream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Config.TOPICS, Config.KAFKA_PARAMS)
    )

    stream
      .map(r => (r.key(), decode[Action](r.value())))
      .filter(entry => entry._1 != null && !entry._1.isEmpty && entry._2.isRight)
      .map(entry => (entry._1, entry._2.right.get))
      .map(e => ((e._1, e._2.time / Config.WATERMARK), e._2.toIpStat))
  }

  def toCassandra(sc: SparkContext, stream: DStream[String]): DStream[String] = {
    import com.datastax.spark.connector.streaming._
    stream
      .map(ip => (ip, Calendar.getInstance().getTimeInMillis))
      .saveToCassandra("fraud_detector", "bots",
        SomeColumns("bot_ip", "time_added"), WriteConf(
          ttl = TTLOption.constant(Config.BOT_IP_CASSANDRA_TTL),
          ifNotExists = true // affects performance?
        )
      )
    stream
  }

  def updateState(ipAndWatermarkedTime: (String, Long),
                  newIpStatOpt: Option[IpStat],
                  state: State[List[(IpStat, Long)]]): (String, List[IpStat]) = {
    val newStats = newIpStatOpt.getOrElse(IpStat.empty())
    if (!state.isTimingOut()) {
      // update state with new data
      state.update(
        if (state.exists) {
          // Get state, filter out old results and append new result to it
          // TODO how to handle timestamp?
          state.get.filter(x => x._2 < (Instant.now.getEpochSecond - 600)) :+ (newStats, ipAndWatermarkedTime._2)
        } else {
          // No previous state, create initial entry
          List((newStats, ipAndWatermarkedTime._2))
        }
      )
    }
    // now we are ready to make our decision
    (ipAndWatermarkedTime._1, state.get map (_._1))
  }

  def findBots(stream: DStream[((String, Long), IpStat)]): DStream[(String, EvaluatedStat)] = {
    stream
      .reduceByKeyAndWindow(
        (l: IpStat, r: IpStat) => l + r,
        Config.DETECTION_WINDOW_INTERVAL,
        Config.DETECTION_SLIDE_INTERVAL
      )
      .mapWithState(StateSpec
        .function(updateState _)
        //.initialState() // TODO can be used to load results from Ignite!
        .timeout(Minutes(10)) // removes users which haven't send any requests for 10 minutes
      )
      .reduceByKey((l: List[IpStat], r: List[IpStat]) => l ++ r)
      .mapValues(EvaluatedStat.classify)
      .filter(stat => stat._2.isBot)
  }

}
