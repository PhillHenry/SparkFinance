package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs.SparkForTests.wholeTextFiles
import com.henryp.sparkfinance.sparkjobs.yahoo.StockSVM.joinByDate
import com.henryp.sparkfinance.sparkjobs.yahoo.YahooIntegrationFixture.{barclaysTicker, dataDirectory, gskTicker, hsbcTicker}
import org.scalatest.{Matchers, WordSpec}

class StockSVMIntegrationSpec extends WordSpec with Matchers {

  "different stocks" should {
    "be converted to a sequence keyed on date" in {
      val dateToFeatures = joinByDate(barclaysTicker, Seq(hsbcTicker, gskTicker), wholeTextFiles(dataDirectory), dayTickerToPrice)
      dateToFeatures.count() should be > 0L
      dateToFeatures.first()._2 should have size 3
    }
  }

}
