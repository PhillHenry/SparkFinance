package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.config.Spark._
import com.henryp.sparkfinance.logging.Logging
import com.henryp.sparkfinance.sparkjobs.SparkForTests._
import com.henryp.sparkfinance.sparkjobs.yahoo.StockSVM._
import com.henryp.sparkfinance.sparkjobs.yahoo.YahooIntegrationFixture._
import org.scalatest.{Matchers, WordSpec}

class StockSVMIntegrationSpec extends WordSpec with Matchers with Logging {

  "different stocks" should {
    "be converted to a sequence keyed on date" in {
      val tickers = Seq(hsbcTicker, standardCharteredTicker, royalBankOfScotland, barclaysTicker)
      val config  = StockCorrelationConfig(directory = dataDirectory(), sparkUrl = localMaster, tickers = tickers)
      val total   = svmForPriceChanges(config, context)
      info(s"total = $total")
      total should not equal 0
    }
  }

}
