import java.time.Instant
import java.util.Calendar

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

object ImplDStreams {

  def run() {
    val (spark, streamingContext) = makeSparkStuff()
    val igniteContext = new IgniteContext(spark.sparkContext, "ignite/config.xml")

    val sharedRDD = igniteContext.fromCache[(String, Long), IpStat]("test")

    val stream = makeFilteredAndParsedLogStream(streamingContext)

    // Save user clicks and views to Ignite to be able to deal with them later
    stream.foreachRDD(rdd => { sharedRDD.savePairs(rdd) })

    val botStream = findBots(stream)

    botStream.print()

    toCassandra(spark.sparkContext, botStream)

    streamingContext.checkpoint("/tmp/spark-checkpoint")
    streamingContext.remember(Config.SPARK_DSTREAM_REMEMBER_INTERVAL)
    streamingContext.start()
    streamingContext.awaitTermination()

    igniteContext.close(true)
  }

  def makeSparkStuff(): (SparkSession, StreamingContext) = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
      .map(e => ((e._1, e._2.time / Config.WATERMARK.milliseconds * 1000), e._2.toIpStat))
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

  def updateState(ipAndWatermarkedTime: (String, Long),
                  newIpStatOpt: Option[IpStat],
                  state: State[List[(IpStat, Long)]]): (String, List[IpStat]) = {
    val newStats = newIpStatOpt.getOrElse(IpStat.empty())
    if (!state.isTimingOut()) {
      // update state with new data
      state.update(
        if (state.exists) {
          // Get state, filter out old results and append new result to it
          state.get.filter(
            x => x._2 < (ipAndWatermarkedTime._2 * (Config.WATERMARK.milliseconds / 1000) - 600)
          ) :+ (newStats, ipAndWatermarkedTime._2)
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
        (l: IpStat, r: IpStat) => l + r, Config.DETECTION_WINDOW_INTERVAL, Config.DETECTION_SLIDE_INTERVAL
      )
      .mapWithState(StateSpec
        .function(updateState _)
        .timeout(Minutes(10)) // removes users which haven't send any requests for 10 minutes
      )
      .reduceByKey((l: List[IpStat], r: List[IpStat]) => l ++ r)
      .mapValues(EvaluatedStat.classify)
      .filter(stat => stat._2.isBot)
  }

}
