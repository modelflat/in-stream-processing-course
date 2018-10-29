import java.sql.Timestamp

import com.datastax.spark.connector.cql.CassandraConnector
import io.circe.parser._
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StringType

object StructuredConfig {
  val WATERMARK = "2 minutes" // allow lateness interval of two mins
  val WINDOW_DURATION = "10 minutes"
  val SLIDE_DURATION = "40 seconds"
  val TRIGGER = "40 seconds"
}

// due to datastax.connector not supporting stream writing (yet), we just make our own ForeachWriter
// of course this is unacceptable for production use. but as a training,
// this is acceptable as data amounts we are dealing with are not very large :)
// also, code and explanation can be found here:
// https://dzone.com/articles/cassandra-sink-for-spark-structured-streaming
class CassandraForEachWriter(val connector: CassandraConnector) extends ForeachWriter[AggregatedLogRecord] {
  val keySpace = "fraud_detector"
  val tableName = "bots_structured"

  private def makeQuery(record: AggregatedLogRecord): String =
    s"""insert into $keySpace.$tableName (bot_ip) values('${record.ip}')"""

  def open(partitionId: Long, version: Long): Boolean = true

  def process(record: AggregatedLogRecord): Unit = {
    connector.withSessionDo(session => session.execute( makeQuery(record) ))
  }

  def close(errorOrNull: Throwable): Unit = { } // nothing to close
}

object ImplStructured {

  def run() {
    val spark = makeSparkStuff()

    val igniteContext = new IgniteContext(spark.sparkContext, "ignite/config.xml")
    val igniteCache = igniteContext.ignite().getOrCreateCache[(String, Timestamp), LogRecord]("UserActionsCache")

    val ds = convertToStructured(spark, readStringsFromKafka(spark))
    ds.printSchema()

    // Save user data to Ignite.
    // TODO search for/think of more efficient saving method? this looks awful
    ds.writeStream.foreach(new ForeachWriter[LogRecord] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: LogRecord): Unit = {
        igniteCache.put((value.ip, value.time), value)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    })
      .trigger(Trigger.ProcessingTime("60 seconds"))

    val botsDS = transformAndFilterBots(spark, ds)
    botsDS.explain(true)

    val exported = toCassandra(spark, botsDS)
      .trigger(Trigger.ProcessingTime(StructuredConfig.TRIGGER))

    exported
      .outputMode(OutputMode.Update())
      .start().awaitTermination()

    igniteContext.close(true)
  }

  def makeSparkStuff(): SparkSession = {
    val spark = SparkSession.builder.master("local[*]")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.cassandra.connection.keep_alive_ms", 120000)
      // cassandra ttl should be 10 minutes
      .config("spark.cassandra.output.ttl", 600)
      // should ignore existing entries while writing into db
      .config("spark.cassandra.output.ifNotExists", "true")
      // local runs may produce exceptions related to ack timeout. in real world
      // we are not likely to be running cassandra on the same machine or even cluster as spark
      .config("spark.cassandra.output.consistency.level", "ANY")
      // Default is 200, which is waaay too much for running locally
      .config("spark.sql.shuffle.partitions", 20)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def readStringsFromKafka(spark: SparkSession)
  : Dataset[(String, String)] = {
    import spark.implicits._
    spark
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", Config.KAFKA_PARAMS("bootstrap.servers").asInstanceOf[String])
      .option("subscribe", Config.TOPICS(0))
      .load()
      .select($"key".cast(StringType).as[String], $"value".cast(StringType).as[String])
  }

  def convertToStructured(spark: SparkSession, ds: Dataset[(String, String)])
  : Dataset[LogRecord] = {
    import spark.implicits._
    ds
      .flatMap(keyVal => {decode[Action](keyVal._2) match {
        case Left(err) =>
          None
        case Right(act) =>
          val ipStat = act.toIpStat
          Some(LogRecord(keyVal._1, new Timestamp(act.time), ipStat.clicks, ipStat.views, act.categoryId))
      }})
      .as[LogRecord]
  }

  def computeStatistics(spark: SparkSession, ds: Dataset[LogRecord])
  : Dataset[AggregatedLogRecord] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    ds
      .withWatermark("time", StructuredConfig.WATERMARK)
      .groupBy($"ip",
        window($"time", StructuredConfig.WINDOW_DURATION, StructuredConfig.SLIDE_DURATION))
      .agg(
        sum($"clicks").alias("clicks"),
        sum($"views").alias("views"),
        collect_set($"category").alias("categories")
      )
      .drop("window")
      .as[AggregatedLogRecord]
  }

  def filterBots(ds: Dataset[AggregatedLogRecord])
  : Dataset[AggregatedLogRecord] = ds.filter(
    alr => BotClassifier.classify(alr.clicks, alr.views, alr.categories.size)._1
  )

  def toCassandra(spark: SparkSession, ds: Dataset[AggregatedLogRecord]): DataStreamWriter[AggregatedLogRecord] = {
    ds.writeStream
      .foreach(new CassandraForEachWriter(CassandraConnector(spark.sparkContext.getConf)))
  }

  def transformAndFilterBots(spark: SparkSession, ds: Dataset[LogRecord])
  : Dataset[AggregatedLogRecord] = filterBots(computeStatistics(spark, ds)).dropDuplicates()

}