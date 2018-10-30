import java.sql.Timestamp

import io.circe.Decoder
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

object Config {
  val BOT_IP_CASSANDRA_TTL = 10.minutes

  val BOT_CATEGORY_LIMIT = 5
  val BOT_REQUEST_LIMIT = 1000
  val BOT_CLICKS_TO_VIEWS_LIMIT = 5
  val BOT_CLICKS_TO_VIEWS_MIN_FRAMES = 5

  val TOPICS = Array("clickstream-log")

  val KAFKA_PARAMS = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
    "group.id" -> "fraud-detector",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer]
  )
}

object BotClassifier {

  def classify(clicks: Long, views: Long, categoriesCount: Long, passedFrames: Long = -1): (Boolean, String) = {

    val strangeClicksToViews =
      if (passedFrames != -1)
        if (passedFrames >= Config.BOT_CLICKS_TO_VIEWS_MIN_FRAMES && views > 0)
          clicks / views > Config.BOT_CLICKS_TO_VIEWS_LIMIT
        else
          false
      else
        clicks / math.max(views, 1) > Config.BOT_CLICKS_TO_VIEWS_LIMIT

    val tooManyRequests = (clicks + views) > Config.BOT_REQUEST_LIMIT
    val tooManyCategories = categoriesCount > Config.BOT_CATEGORY_LIMIT

    (
      tooManyRequests || tooManyCategories || strangeClicksToViews,
      if (tooManyRequests)      "requests"     else
      if (tooManyCategories)    "categories"   else
      if (strangeClicksToViews) "clicks/views" else
        "clear"
    )
  }

}

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

case class EvaluatedStat(originalStat: IpStat, isBot: Boolean, reason: String)
object EvaluatedStat {
  def classify(ipStat: List[IpStat]): EvaluatedStat = {
    val aggr = ipStat reduce (_+_)
    val res = BotClassifier.classify(aggr.clicks, aggr.views, aggr.categories.size)
    EvaluatedStat(aggr, res._1, res._2)
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
object Action {
  implicit val decoder: Decoder[Action] =
    Decoder.forProduct3("time", "categoryId", "action")(Action.apply)
}

case class LogRecord(ip: String, time: Timestamp, clicks: Long, views: Long, category: String)

case class AggregatedLogRecord(ip: String, clicks: Long, views: Long, categories: Set[String])
