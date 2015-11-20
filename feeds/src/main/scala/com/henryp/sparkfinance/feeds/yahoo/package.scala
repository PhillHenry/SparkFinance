package com.henryp.sparkfinance.feeds

import com.henryp.sparkfinance.feeds.DateParsing.toDaysFromEpoch

package object yahoo {

  type TickerDate = String

  type DateTickerPriceVolume[T] = ((T, String), Double, Double)

  type DateTickerPrice[T] = ((T, String), Double)

  def isNotMeta(line: String): Boolean = !line.startsWith("Date")

  def dateTickerToPriceVolume[T](ticker: String, line: String): DateTickerPriceVolume[T]
    = ((date(line).asInstanceOf[T], ticker), closingPrice(line), volume(line))

  def dayTickerToPrice[T](ticker: String, line: String): DateTickerPrice[T]
    = ((toDaysFromEpoch(date(line)).asInstanceOf[T], ticker), closingPrice(line))

  def dateTickerToPrice[T](ticker: String, line: String): DateTickerPrice[T]
    = ((date(line).asInstanceOf[T], ticker), closingPrice(line))

  def asDateToPrice[T](kv: DateTickerPrice[T]): (T, Double)
    = (kv._1._1, kv._2)

  def lineToDateAndClosePrice(line: String): (String, Double)
    = (date(line), closingPrice(line))

  def closingPrice(line: String): Double = elements(line)(4).toDouble

  def date(line: String): TickerDate = elements(line)(0)

  def volume(line: String): Double = elements(line)(5).toDouble

  def elements(line: String): Array[String] = line.split(",")

}
