package com.henryp.sparkfinance.feeds

package object yahoo {

  type TickerDate = String

  def isNotMeta(line: String): Boolean = !line.startsWith("Date")

  def lineToDateAndClosePrice(line: String): (String, Double) = {
    (date(line), closingPrice(line))
  }

  def closingPrice(line: String): Double = elements(line)(4).toDouble

  def date(line: String): TickerDate = elements(line)(0)

  def volume(line: String): Double = elements(line)(5).toDouble

  def elements(line: String): Array[String] = line.split(",")

}
