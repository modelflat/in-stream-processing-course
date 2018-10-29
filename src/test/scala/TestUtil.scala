import java.time.Instant
import java.time.temporal.ChronoUnit
import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object TestUtil {

  def randomAction(viewProb: Double = 0.5)
  : String = if (math.random < viewProb) "view" else "click"

  def past(interval: Long)
  : Long = Instant.now().minus(interval, ChronoUnit.SECONDS).getEpochSecond

  def generateRequestsPerInterval(ip: String, requests: Int, interval: Int = 600)
  : IndexedSeq[(String, Action)] = {
    val baseTime = past(interval)
    (0 until requests)
      .map(i => (
        ip,
        new Action(baseTime + (interval / requests) * i, "SomeCat", randomAction())
      ))
  }

  def generateClicksToViewsPerInterval(ip: String, requests: Int, clicksToViews: Int, interval: Int = 600)
  : IndexedSeq[(String, Action)] = {
    val views = requests / (clicksToViews + 1)
    val clicks = requests - views
    val baseTime = past(interval)
    (0 until views)
      .map({ i => (
        ip,
        new Action(baseTime + (interval / requests) * i, "SomeCat", "view")
      )}) ++
    (0 until clicks).map({ i => (
      ip,
      new Action(baseTime + (interval / requests) * i, "SomeCat", "click")
    )})
  }

  def generateCategoriesPerInterval(ip: String, requests: Int, differentCategories: Int, interval: Int = 600)
  : IndexedSeq[(String, Action)] = {
    if (requests < differentCategories) {
      throw new RuntimeException("Requests should be > differentCats")
    }
    val baseTime = past(interval)
    val cats = (0 until differentCategories).map(i => f"cat$i")
    (0 until requests).map(i => (
      ip,
      new Action(baseTime + (interval / requests) * i, cats(i.intValue() % cats.length), randomAction())
    ))
  }

  def makeDStream(sc: SparkContext, ssc: StreamingContext, input: IndexedSeq[(String, Action)])
  : DStream[(String, Action)] = ssc.queueStream(mutable.Queue(sc.parallelize(input)))

  def convertToLogRecords(in: IndexedSeq[(String, Action)])
  : IndexedSeq[LogRecord] = in map (x => LogRecord(
    x._1, new Timestamp(x._2.time),
    if (x._2.action == "click") 1 else 0, if (x._2.action == "view") 1 else 0,
    x._2.categoryId
  ))

}
