import java.sql.Timestamp
import java.time.Instant

import com.datastax.spark.connector.cql.CassandraConnector
import io.circe.parser._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StringType

// due to datastax.connector not supporting stream writing (yet), we just make our own ForeachWriter
// of course this is unacceptable for production use. but as a training,
// this is acceptable as data amounts we are dealing with are not very large :)
// also, the original code can be found here:
// https://github.com/polomarcus/Spark-Structured-Streaming-Examples/blob/master/src/main/scala/cassandra/foreachSink/CassandraSinkForeach.scala
class CassandraForEachWriter(val connector: CassandraConnector) extends ForeachWriter[AggregatedLogRecord] {
  val keySpace = "fraud_detector"
  val tableName = "bots_structured"

  private def makeQuery(record: AggregatedLogRecord): String =
    s"""insert into $keySpace.$tableName (bot_ip, time_added) values('${record.ip}', '${Instant.now().getEpochSecond}')"""

  def open(partitionId: Long, version: Long): Boolean = true

  def process(record: AggregatedLogRecord): Unit = {
    connector.withSessionDo(session => session.execute( makeQuery(record) ))
  }

  def close(errorOrNull: Throwable): Unit = { } // nothing to close
}

object ImplStructured {

  def run() {
    val spark = makeSparkStuff()

    val ds = readStructuredDS(spark)
    ds.printSchema()

    val groupedDS = computeStatistics(spark, ds)
    groupedDS.printSchema()

    val botsDS = filterBots(spark, groupedDS)

    val exported = toCassandra(spark, botsDS)

    exported.start().awaitTermination()
  }

  def makeSparkStuff(): SparkSession = {
    val spark = SparkSession.builder.master("local[*]")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.cassandra.connection.keep_alive_ms", 120000)
      .config("spark.cassandra.output.ttl", 600)
      .config("spark.cassandra.output.ifNotExists", "true")
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

  def toCassandra(spark: SparkSession, ds: Dataset[AggregatedLogRecord]): DataStreamWriter[AggregatedLogRecord] = {
    ds.writeStream.foreach(new CassandraForEachWriter(CassandraConnector(spark.sparkContext.getConf)))
  }

}