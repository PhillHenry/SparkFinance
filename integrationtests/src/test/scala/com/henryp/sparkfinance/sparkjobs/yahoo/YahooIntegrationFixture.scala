package com.henryp.sparkfinance.sparkjobs.yahoo

object YahooIntegrationFixture {

  val hsbcTicker = "HSBA"

  val barclaysTicker = "BARC"

  val gskTicker = "GSK"

  def dataDirectory = (this.getClass.getResource("/") + "../../src/test/resources/tickers/").replaceFirst("^file:", "file://")

}
