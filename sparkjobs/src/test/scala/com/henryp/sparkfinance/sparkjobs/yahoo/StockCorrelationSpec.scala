package com.henryp.sparkfinance.sparkjobs.yahoo

import org.scalatest.{Matchers, WordSpec}

class StockCorrelationSpec extends WordSpec with Matchers {

  "all combination of tickers" should {
    "be compared" in {
      val allPairs = StockCorrelation.comparisonPairs(List("1", "2", "3", "4", "5"))
      allPairs should have size (4+3+2+1)
    }
  }

  "args" should {
    "be parsed" in {
      val directory = "directory"
      val sparkUrl = "sparkConfig"
      val configOption = StockCorrelation.parseArgs(Array("-d", directory, "-t", "ticker1,ticker2", "-s", sparkUrl))
      configOption.orElse(???).map { config =>
        config.directory shouldEqual directory
        config.sparkUrl shouldEqual sparkUrl
        config.tickers should have size 2
        config
      }
    }
  }

}
