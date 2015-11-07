package com.henryp.sparkfinance.sparkjobs

import com.henryp.sparkfinance.config.Spark
import com.henryp.sparkfinance.feeds.yahoo._
import org.scalatest.{Matchers, WordSpec}

class SparkSpec extends WordSpec with Matchers {

  "all information loaded" should {
    "be joinable" in {
      val context = Spark.sparkContext()

      val dataDirectory = (this.getClass.getResource("/") + "../../src/test/resources/tickers/").replaceFirst("^file:", "file://")
      val all           = context.wholeTextFiles(dataDirectory)
      val aggregated    = aggregate(all, isNotMeta, dateTickerKey)
      val hsba          = aggregated.filter(matchesTicker("HSBA.L", _))
      val barc          = aggregated .filter(matchesTicker("BARC.L", _))

      hsba.count() shouldEqual 9 // 10 lines - 1 meta data line
      barc.count() shouldEqual 9 // ditto

      // TODO more assertions

      context.stop()
    }
  }

}
