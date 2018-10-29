import ImplStructured.transformAndFilterBots
import com.holdenkarau.spark.testing.StructuredStreamingBase
import org.scalatest._

class StructuredBotDetectionTest extends FlatSpec with StructuredStreamingBase {

  it should "mark user as bot if they produce more than 1000 requests per 10 minutes" in {
    import spark.implicits._

    val input = TestUtil.convertToLogRecords(
      TestUtil.generateRequestsPerInterval("bot", Config.BOT_REQUEST_LIMIT + 1) ++
      TestUtil.generateRequestsPerInterval("hum", Config.BOT_REQUEST_LIMIT - 1)
    )

    val ds = spark.sqlContext.createDataset(input)

    val bots = transformAndFilterBots(spark, ds).collect().toList

    assert( bots.size == 1 )
    assert( bots.head.ip == "bot" )
  }

  it should "mark user as bot if they visit more than 5 categories per 10 minutes" in {
    import spark.implicits._

    val input = TestUtil.convertToLogRecords(
      TestUtil.generateCategoriesPerInterval("bot", 200, Config.BOT_CLICKS_TO_VIEWS_LIMIT + 1) ++
      TestUtil.generateCategoriesPerInterval("hum", 200, Config.BOT_CLICKS_TO_VIEWS_LIMIT - 1)
    )

    val ds = spark.sqlContext.createDataset(input)

    val bots = transformAndFilterBots(spark, ds).collect().toList

    assert( bots.size == 1 )
    assert( bots.head.ip == "bot" )
  }

  it should "mark user as bot if they have clicks/views ratio higher than 5" in {
    import spark.implicits._

    val input = TestUtil.convertToLogRecords(
      TestUtil.generateClicksToViewsPerInterval("bot", 200, Config.BOT_CATEGORY_LIMIT + 1) ++
      TestUtil.generateClicksToViewsPerInterval("hum", 200, Config.BOT_CATEGORY_LIMIT - 1)
    )

    val ds = spark.sqlContext.createDataset(input)

    val bots = transformAndFilterBots(spark, ds).collect().toList

    assert( bots.size == 1 )
    assert( bots.head.ip == "bot" )
  }

}