package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.config.Spark._
import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs.SparkForTests._
import com.henryp.sparkfinance.sparkjobs._
import com.henryp.sparkfinance.sparkjobs.yahoo.StockSVM._
import com.henryp.sparkfinance.sparkjobs.yahoo.YahooIntegrationFixture._
import org.scalatest.{Matchers, WordSpec}

class StockSVMIntegrationSpec extends WordSpec with Matchers {

  "different stocks" should {
    "be converted to a sequence keyed on date" in {
      val aggregated      = aggregate(wholeTextFiles(dataDirectory()), isNotMeta, parser)
      val tickers         = Seq(hsbcTicker, standardCharteredTicker, royalBankOfScotland, barclaysTicker)
      val dateToFeatures  = joinByDate(tickers, aggregated, asDateToDouble[Int])
      dateToFeatures.count() should be > 0L
      dateToFeatures.first()._2 should have size tickers.size

      // TEST
      val config = StockCorrelationConfig(directory = dataDirectory(), sparkUrl = localMaster, tickers = tickers)
      svmForPriceChanges(config, context) should not equal 0
    }
  }

}
