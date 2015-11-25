package com.henryp.sparkfinance.sparkjobs.yahoo

object YahooIntegrationFixture {

  val hsbcTicker = "HSBA"

  val barclaysTicker = "BARC"

  val gskTicker = "GSK"

  val standardCharteredTicker = "STAN"

  val royalBankOfScotland = "RBS"

  def dataDirectory() = (this.getClass.getResource("/") + "../../src/test/resources/tickers/").replaceFirst("^file:", "file://")

}
