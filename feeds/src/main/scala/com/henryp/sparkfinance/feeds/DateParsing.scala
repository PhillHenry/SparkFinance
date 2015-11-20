package com.henryp.sparkfinance.feeds

import java.util.Calendar
import java.util.Calendar.{DAY_OF_MONTH, MONTH, YEAR}

object DateParsing {

  final val msInDay = 24 * 60 * 60 * 1000

  def toDaysFromEpoch(date: String): Int = {
    val dayOfMonth = yyyymmddToDay(date).toInt
    val month = yyyymmddToMonth(date).toInt - 1 // months are 0 based
    val year = yyyymmddToYear(date).toInt

    val calendar = Calendar.getInstance()
    calendar.set(YEAR, year)
    calendar.set(MONTH, month)
    calendar.set(DAY_OF_MONTH, dayOfMonth)

    (calendar.getTime.getTime / msInDay).asInstanceOf[Int]
  }

  def yyyymmddToDay(date: String): String = date.substring(8, 10)
  def yyyymmddToMonth(date: String): String = date.substring(5, 7)
  def yyyymmddToYear(date: String): String = date.substring(0, 4)

  def mmddyyyyToDay(mmddyyyy: String): String = mmddyyyy.substring(3,5)
  def mmddyyyyToMonth(mmddyyyy: String): String = mmddyyyy.substring(0,2)
  def mmddyyyyToYear(mmddyyyy: String): String = mmddyyyy.substring(6,10)


}
