package com.henryp.sparkfinance.feeds

package object yahoo {

  type TickerDate = String

  type DateTickerPriceVolume = ((TickerDate, String), Double, Double)

  def isNotMeta(line: String): Boolean = !line.startsWith("Date")

  def dateTickerKey(ticker: String, line: String): DateTickerPriceVolume = ((date(line), ticker), closingPrice(line), volume(line))

  def matchesTicker(ticker: String, tuple: DateTickerPriceVolume) = tuple._1._2.contains(ticker) // TODO remove the directory

  def lineToDateAndClosePrice(line: String): (String, Double) = {
    (date(line), closingPrice(line))
  }

  def closingPrice(line: String): Double = elements(line)(4).toDouble

  def date(line: String): TickerDate = elements(line)(0)

  def volume(line: String): Double = elements(line)(5).toDouble

  def elements(line: String): Array[String] = line.split(",")

}
