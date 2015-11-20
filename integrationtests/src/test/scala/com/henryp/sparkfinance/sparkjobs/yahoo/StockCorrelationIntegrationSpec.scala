package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds._
import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import com.henryp.sparkfinance.sparkjobs.yahoo.YahooIntegrationFixture.{barclaysTicker, dataDirectory, hsbcTicker}
import org.scalatest.{Matchers, WordSpec}

class StockCorrelationIntegrationSpec extends WordSpec with Matchers {

  "pearson correlations" should {
    "be generated" in {
      val config = StockCorrelationConfig(directory = dataDirectory, tickers=List("HSBA", "BARC"))
      val correlations = StockCorrelation.doCorrelations(config, { config => config.stop() })
      correlations should have size 1
    }
  }

  "all information loaded" should {
    "be joinable" in {

      val all           = SparkForTests.wholeTextFiles(dataDirectory)
      val aggregated    = aggregate(all, isNotMeta, dateTickerToPrice)
      val hsba          = aggregated.filter(matchesTicker[DateTickerPrice[TickerDate]](hsbcTicker, _))
      val barc          = aggregated.filter(matchesTicker[DateTickerPrice[TickerDate]](barclaysTicker, _))

      hsba.count() shouldEqual 9 // 10 lines - 1 meta data line
      barc.count() shouldEqual 9 // ditto

    }
  }



}
