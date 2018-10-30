import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import io.circe.parser._
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

object DStreamConfig {
  val DETECTION_SLIDE_INTERVAL = Seconds(30)
  val DETECTION_WINDOW_INTERVAL = Seconds(90)
  val BATCH_INTERVAL = Seconds(30)
  val REMEMBER_INTERVAL = Seconds(35)
  val TRACK_DURATION = Minutes(10)
}

object ImplDStreams {

  def run() {
    val (spark, streamingContext) = makeSparkStuff()

    val igniteContext = new IgniteContext(spark.sparkContext, "ignite/config.xml")
    val sharedRDD = igniteContext.fromCache[String, Action]("test")

    val rawStream = makeKafkaDirectStream(streamingContext)
    // Save user clicks and views to Ignite to be able to deal with them later
    rawStream.foreachRDD(rdd => { sharedRDD.savePairs(rdd) })

    val botStream = transformAndFindBots(rawStream)
    botStream.print()

    toCassandra(spark.sparkContext, botStream)

    streamingContext.checkpoint("/tmp/spark-checkpoint")
    streamingContext.remember(DStreamConfig.REMEMBER_INTERVAL)
    streamingContext.start()
    streamingContext.awaitTermination()

    igniteContext.close(true)
  }

  def makeSparkStuff(master: String = "local[*]"): (SparkSession, StreamingContext) = {
    val spark = SparkSession.builder
      .master(master)
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.cassandra.connection.keep_alive_ms", 120000)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(spark.sparkContext, DStreamConfig.BATCH_INTERVAL)
    (spark, streamingContext)
  }

  def makeKafkaDirectStream(streamingContext: StreamingContext): DStream[(String, Action)] = {
    KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Config.TOPICS, Config.KAFKA_PARAMS)
    ) .map(r => (r.key(), decode[Action](r.value())))
      .filter(entry => entry._1 != null && !entry._1.isEmpty && entry._2.isRight)
      .map(entry => (entry._1, entry._2.right.get))
  }

  def toCassandra(sc: SparkContext, stream: DStream[(String, EvaluatedStat)]): DStream[(String, EvaluatedStat)] = {
    import com.datastax.spark.connector.streaming._
    stream
      .map(ip => (ip._1, ip._2.reason))
      .saveToCassandra("fraud_detector", "bots",
        SomeColumns("bot_ip", "reason"), WriteConf(
          ttl = TTLOption.constant(Config.BOT_IP_CASSANDRA_TTL),
          ifNotExists = true // affects performance?
        )
      )
    stream
  }

  def bucketTime(time: Long)
  : Long = time / DStreamConfig.DETECTION_SLIDE_INTERVAL.milliseconds * 1000

  def unbucketTime(time: Long)
  : Long = time * DStreamConfig.DETECTION_SLIDE_INTERVAL.milliseconds / 1000

  def transformActions(stream: DStream[(String, Action)]): DStream[((String, Long), IpStat)] = {
    stream.map(e => ((e._1, bucketTime(e._2.time)), e._2.toIpStat))
  }

  def updateState(ipAndBucketedTime: (String, Long),
                  newIpStatOpt: Option[IpStat],
                  state: State[List[(IpStat, Long)]]): (String, List[IpStat]) = {
    val newStats = newIpStatOpt.getOrElse(IpStat.empty())
    if (!state.isTimingOut) {
      // update state with new data
      state.update(
        if (state.exists) {
          // Get state, filter out old results and append new result to it
          state.get.filter(
            x => x._2 < ( unbucketTime(ipAndBucketedTime._2) - DStreamConfig.TRACK_DURATION.milliseconds / 1000 )
          ) :+ (newStats, unbucketTime(ipAndBucketedTime._2))
        } else {
          // No previous state, create initial entry
          List((newStats, ipAndBucketedTime._2))
        }
      )
    }
    // now we are ready to make our decision
    (ipAndBucketedTime._1, state.get map (_._1))
  }

  def findBots(stream: DStream[((String, Long), IpStat)]): DStream[(String, EvaluatedStat)] = {
    stream
      .reduceByKeyAndWindow(
        (l: IpStat, r: IpStat) => l + r,
        DStreamConfig.DETECTION_WINDOW_INTERVAL,
        DStreamConfig.DETECTION_SLIDE_INTERVAL
      )
      .mapWithState(StateSpec
        .function(updateState _)
        .timeout(DStreamConfig.TRACK_DURATION) // removes users which haven't send any requests for 10 minutes
      )
      .reduceByKey((l: List[IpStat], r: List[IpStat]) => l ++ r)
      .mapValues(EvaluatedStat.classify)
      .filter(stat => stat._2.isBot)
  }

  def transformAndFindBots(stream: DStream[(String, Action)]): DStream[(String, EvaluatedStat)] = {
    findBots(transformActions(stream))
  }

}
