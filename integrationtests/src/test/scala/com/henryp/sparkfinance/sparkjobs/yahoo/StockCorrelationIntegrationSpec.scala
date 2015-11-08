package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.config.Spark
import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import org.scalatest.{Matchers, WordSpec}

class StockCorrelationIntegrationSpec extends WordSpec with Matchers {

  "pearson correlations" should {
    "be generated" in {
      val correlations = StockCorrelation.doCorrelations(StockCorrelationConfig(directory = dataDirectory, tickers=List("HSBA", "BARC")))
      correlations should have size 1
    }
  }

  "all information loaded" should {
    "be joinable" in {
      val context       = Spark.sparkContext()
      val all           = context.wholeTextFiles(dataDirectory)
      val aggregated    = aggregate(all, isNotMeta, dateTickerToPriceVolume)
      val hsba          = aggregated.filter(matchesTicker("HSBA", _))
      val barc          = aggregated.filter(matchesTicker("BARC", _))

      hsba.count() shouldEqual 9 // 10 lines - 1 meta data line
      barc.count() shouldEqual 9 // ditto

      // TODO more assertions

      context.stop()
    }
  }

  def dataDirectory = (this.getClass.getResource("/") + "../../src/test/resources/tickers/").replaceFirst("^file:", "file://")

}
