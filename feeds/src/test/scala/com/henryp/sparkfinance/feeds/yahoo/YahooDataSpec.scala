package com.henryp.sparkfinance.feeds.yahoo

import org.scalatest.{Matchers, WordSpec}

class YahooDataSpec extends WordSpec with Matchers {

  /**
   * Date,Open,High,Low,Close,Volume,Adj Close
   */
  val typicalLine = "2015-08-19,272.65,273.00,267.55,267.90,26889500,267.90"

  "date" should {
    "be extracted" in {
      date(typicalLine) shouldEqual "2015-08-19"
    }
  }

  "volume" should {
    "be extracted" in {
      volume(typicalLine) shouldEqual 26889500
    }
  }

  "closing price" should {
    "be extracted" in {
      closingPrice(typicalLine) shouldEqual 267.90
    }
  }

}