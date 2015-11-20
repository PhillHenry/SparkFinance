package com.henryp.sparkfinance.feeds

import com.henryp.sparkfinance.feeds.DateParsing.toDaysFromEpoch

package object yahoo {

  type TickerDate = String

  type DateTickerPriceVolume = ((TickerDate, String), Double, Double)

  type DayTickerPriceVolume = ((Int, String), Double, Double)

  type DateTickerPrice = ((TickerDate, String), Double)

  def isNotMeta(line: String): Boolean = !line.startsWith("Date")

  def dateTickerToPriceVolume(ticker: String, line: String): DateTickerPriceVolume = ((date(line), ticker), closingPrice(line), volume(line))

  def dayTickerToPriceVolume(ticker: String, line: String): DayTickerPriceVolume = ((toDaysFromEpoch(date(line)), ticker), closingPrice(line), volume(line))

  def dateTickerToPrice(ticker: String, line: String): DateTickerPrice = ((date(line), ticker), closingPrice(line))

  def asDateToPrice(kv: DateTickerPrice): (TickerDate, Double) = (kv._1._1, kv._2)

  def lineToDateAndClosePrice(line: String): (String, Double) = {
    (date(line), closingPrice(line))
  }

  def closingPrice(line: String): Double = elements(line)(4).toDouble

  def date(line: String): TickerDate = elements(line)(0)

  def volume(line: String): Double = elements(line)(5).toDouble

  def elements(line: String): Array[String] = line.split(",")

}
