package com.henryp.sparkfinance.feeds.yahoo

import com.henryp.sparkfinance.feeds.yahoo.YahooFinance.{parseEndDate, parseStartDate}
import org.scalatest.{Matchers, WordSpec}

class YahooDataSpec extends WordSpec with Matchers {

  /**
   * Date,Open,High,Low,Close,Volume,Adj Close
   */
  val typicalLine = "2015-08-19,272.65,273.00,267.55,267.90,26889500,267.90"

  "date switched" should {
    "be parsed for a URL" in {
      parseStartDate("07-19-2007") shouldEqual "a=07&b=19&c=2007"
      parseEndDate("07-19-2015") shouldEqual "d=07&e=19&f=2015"
    }
  }

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

  "diffs" should {
    "be included" in {
      val extractor = new YahooDataExtractor(new YahooFinanceConfig)
      val line1     = "2015-08-19,551.00,551.33,539.97,540.60,28985900,540.60"
      val line2     = "2015-08-18,555.30,558.20,553.10,556.00,17348600,556.00"
      extractor.toLine(line1, None) shouldEqual line1 + ",0"

      val expectedDiff  = "15.40"
      val output        = extractor.toLine(line2, Some(line1))
      output shouldEqual line2 + s",$expectedDiff"
      closingPriceChangeElement(output) shouldEqual expectedDiff
    }
  }

}
