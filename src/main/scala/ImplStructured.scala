import org.apache.spark.sql._
import io.circe.parser._
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp

import org.apache.spark.sql.streaming.OutputMode

object ImplStructured {

  def run() {

    val slidingWindowSize = "10 minutes"
    val slideSize = "30 seconds"
    val watermark = "30 seconds"

    val spark = makeSparkStuff()

    val ds = readStructuredDS(spark)
    ds.printSchema()

    val groupedDS = computeStatistics(spark, ds)
    groupedDS.printSchema()

    val botsDS = filterBots(spark, groupedDS)

    botsDS.writeStream
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "fraud_detector")
      .option("table", "bots_structured")
      .outputMode(OutputMode.Update())

      .start()
      .awaitTermination()

  }

  def makeSparkStuff(): SparkSession = {
    val spark = SparkSession.builder.master("local[*]")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.cassandra.connection.keep_alive_ms", 120000)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def readStructuredDS(spark: SparkSession): Dataset[LogRecord] = {
    import spark.implicits._
    spark
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", Config.KAFKA_PARAMS("bootstrap.servers").asInstanceOf[String])
      .option("subscribe", Config.TOPICS(0))
      .load()
      .select($"key".cast(StringType).as[String], $"value".cast(StringType).as[String])
      .flatMap(keyVal => {decode[Action](keyVal._2) match {
        case Left(err) =>
          None
        case Right(act) =>
          val ipStat = act.toIpStat
          Some(LogRecord(keyVal._1, new Timestamp(act.time), ipStat.clicks, ipStat.views, act.categoryId))
      }})
      .as[LogRecord]
  }

  def computeStatistics(spark: SparkSession, ds: Dataset[LogRecord]): Dataset[AggregatedLogRecord] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    // TODO move magic values to config
    ds
      .withWatermark("time", "30 seconds")
      .groupBy(window($"time", "10 minutes", "30 seconds"), $"ip")
      .agg(
        sum($"clicks").alias("clicks"),
        sum($"views").alias("views"),
        collect_set($"category").alias("categories")
      )
      .drop("window")
      .as[AggregatedLogRecord]
  }

  def filterBots(spark: SparkSession, ds: Dataset[AggregatedLogRecord]): Dataset[AggregatedLogRecord] = {
    ds.filter(alr => BotClassifier.classify(alr.clicks, alr.views, alr.categories.size)._1)
  }

}