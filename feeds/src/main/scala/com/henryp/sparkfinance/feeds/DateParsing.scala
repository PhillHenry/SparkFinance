package com.henryp.sparkfinance.feeds

import java.util.Calendar
import java.util.Calendar.{DAY_OF_MONTH, MONTH, YEAR}

object DateParsing {

  final val msInDay = 24 * 60 * 60 * 1000

  def toDaysFromEpoch(mmddyyyy: String): Int = {
    val dayOfMonth = mmddyyyyToDay(mmddyyyy).toInt
    val month = mmddyyyyToMonth(mmddyyyy).toInt - 1 // months are 0 based
    val year = mmddyyyyToYear(mmddyyyy).toInt

    val calendar = Calendar.getInstance()
    calendar.set(YEAR, year)
    calendar.set(MONTH, month)
    calendar.set(DAY_OF_MONTH, dayOfMonth)

    (calendar.getTime.getTime / msInDay).asInstanceOf[Int]
  }

  def mmddyyyyToDay(mmddyyyy: String): String = mmddyyyy.substring(3,5)

  def mmddyyyyToMonth(mmddyyyy: String): String = mmddyyyy.substring(0,2)

  def mmddyyyyToYear(mmddyyyy: String): String = mmddyyyy.substring(6,10)


}
