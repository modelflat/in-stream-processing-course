import ImplDStreams.transformAndFindBots
import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FlatSpec

class DStreamBotDetectionTest extends FlatSpec with StreamingSuiteBase {

  override val batchDuration = Config.SPARK_BATCH_INTERVAL

  def operation(in: DStream[(String, Action)]): DStream[(String, String)] = {
    transformAndFindBots(in).map(el => (el._1, el._2.reason))
  }

  it should s"mark user as bot if they produce more than ${Config.BOT_REQUEST_LIMIT} requests per 10 minutes" in {
    val input =
      TestUtil.generateRequestsPerInterval("bot", Config.BOT_REQUEST_LIMIT + 1) ++
      TestUtil.generateRequestsPerInterval("hum", Config.BOT_REQUEST_LIMIT - 1)

    val expected = List( ("bot", "requests") )

    testOperation[(String, Action), (String, String)](List(input), operation _, List(expected))
  }

  it should s"mark user as bot if they visit more than ${Config.BOT_CATEGORY_LIMIT} categories per 10 minutes" in {
    val input =
      TestUtil.generateCategoriesPerInterval("bot", 200, Config.BOT_CATEGORY_LIMIT + 1) ++
      TestUtil.generateCategoriesPerInterval("hum", 200, Config.BOT_CATEGORY_LIMIT - 1)

    val expected = List( ("bot", "categories") )

    testOperation[(String, Action), (String, String)](List(input), operation _, List(expected))
  }

  it should s"mark user as bot if they have clicks/views ratio higher than ${Config.BOT_CLICKS_TO_VIEWS_LIMIT}" in {
    val input =
      TestUtil.generateClicksToViewsPerInterval("bot", 200, Config.BOT_CLICKS_TO_VIEWS_LIMIT + 1) ++
      TestUtil.generateClicksToViewsPerInterval("hum", 200, Config.BOT_CLICKS_TO_VIEWS_LIMIT - 1)

    val expected = List( ("bot", "clicks/views") )

    testOperation[(String, Action), (String, String)](List(input), operation _, List(expected))
  }

}