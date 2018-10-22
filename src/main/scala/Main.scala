import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

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

case class EvaluatedStat(ip: String, originalStat: IpStat, isBot: Boolean, reason: String)
object EvaluatedStat {
  def classify(ip: String, ipStat: IpStat): EvaluatedStat = {
    val tooManyRequests = (ipStat.clicks + ipStat.views) > 1000
    val strangeClicksToViews = (ipStat.clicks.toDouble / math.max(ipStat.views, 0.8)) > 5
    val tooManyCategories = ipStat.categories.size > 10
    EvaluatedStat(ip, ipStat,
      tooManyRequests || tooManyCategories || strangeClicksToViews,
      if (tooManyRequests) "requests" else
      if (tooManyCategories) "categories" else
      if (strangeClicksToViews) "clicks/views" else
        "clear"
    )
  }
}

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

object Main extends App {
  val (spark, streamingContext) = makeSparkStuff()
  val stream = makeFilteredAndParsedLogStream(streamingContext)

  streamingContext.checkpoint("/tmp/spark-checkpoint")

  aggregatedActions(stream)

  streamingContext.remember(Config.SPARK_DSTREAM_REMEMBER_INTERVAL)
  streamingContext.start()
  streamingContext.awaitTermination()

  def makeSparkStuff(): (SparkSession, StreamingContext) = {
    val spark = SparkSession.builder.master("local[*]")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(spark.sparkContext, Config.SPARK_BATCH_INTERVAL)
    (spark, streamingContext)
  }

  def makeFilteredAndParsedLogStream(streamingContext: StreamingContext): DStream[(String, IpStat)] = {
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext, PreferConsistent, Subscribe[String, String](Config.TOPICS, Config.KAFKA_PARAMS)
    )

    stream
      .map(r => (r.key(), decode[Action](r.value())))
      .filter(entry => entry._1 != null && !entry._1.isEmpty && entry._2.isRight)
      .map(entry => (entry._1, entry._2.right.get.toIpStat))
  }

  /**
    * State should keep the list of IpStats aggregated so far.
    */
  def updateState(ip: String, newIpStatOpt: Option[IpStat], state: State[List[IpStat]]): EvaluatedStat = {
    val newStats = newIpStatOpt.getOrElse(IpStat.empty())
    if (!state.isTimingOut()) {
      // update state with new data
      // TODO implement expiration?
      state.update(if (state.exists) state.get :+ newStats else List(newStats))
      // now we are ready to make our decisions
    }
    EvaluatedStat.classify(ip, state.get.reduce(_ + _))
  }

  def aggregatedActions(stream: DStream[(String, IpStat)]): Unit = {
    stream
      // aggregate new IpStat for each ip once a 30 seconds // TODO to config
      .reduceByKeyAndWindow((l: IpStat, r: IpStat) => l + r, Seconds(30), Seconds(30))
      .mapWithState(StateSpec
        .function(updateState _)
        //.initialState() // TODO can be used to load results from Ignite!
        .timeout(Minutes(10)) // removes users which haven't send any requests for 10 minutes
      )
      .filter(stat => stat.isBot)
      .print()
  }

}
