package com.henryp.sparkfinance.feeds

import java.math.BigDecimal

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

  def dayTickerToPriceChange[T](ticker: String, line: String): DateTickerPrice[T]
  = ((toDaysFromEpoch(date(line)).asInstanceOf[T], ticker), closingPriceChange(line))

  def dateTickerToPrice[T](ticker: String, line: String): DateTickerPrice[T]
    = ((date(line).asInstanceOf[T], ticker), closingPrice(line))

  def asDateToDouble[T](kv: DateTickerPrice[T]): (T, Double)
    = (kv._1._1, kv._2)

  def lineToDateAndClosePrice(line: String): (String, Double)
    = (date(line), closingPrice(line))

  def closingPrice(line: String): Double =
    closingPriceElement(line).toDouble

  def closingPriceChange(line: String): Double =
    closingPriceChangeElement(line).toDouble

  def closingPriceElement(line: String): String =
    elements(line)(4)

  def closingPriceChangeElement(line: String): String =
    elements(line)(7)

  def date(line: String): TickerDate = elements(line)(0)

  def volume(line: String): Double = elements(line)(5).toDouble

  def elements(line: String): Array[String] = line.split(",")

  def diff(element: String, toSubtract: String): String = {
    new BigDecimal(element).subtract(new BigDecimal(toSubtract)).toString
  }

}
