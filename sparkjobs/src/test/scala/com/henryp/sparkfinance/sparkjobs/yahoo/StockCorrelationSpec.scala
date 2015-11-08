package com.henryp.sparkfinance.sparkjobs.yahoo

import org.scalatest.{Matchers, WordSpec}

class StockCorrelationSpec extends WordSpec with Matchers {

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
