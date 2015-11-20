package com.henryp.sparkfinance.feeds

import com.henryp.sparkfinance.feeds.DateParsing._
import org.scalatest.{Matchers, WordSpec}

class DateParsingSpec extends WordSpec with Matchers {

  "01-01-1970" should {
    "be start of epoch" in {
      toDaysFromEpoch("01-01-1970") shouldEqual 0
    }
  }

  "31 January 1970" should {
    "be 30 days into epoch because we round down" in {
      toDaysFromEpoch("01-31-1970") shouldEqual 30
    }
  }

  "1 February 1970" should {
    "should be 31 days into epoch because we round down" in {
      toDaysFromEpoch("02-01-1970") shouldEqual 31
    }
  }

  "mm-dd-yyyy" should {
    "break down into day month year" in {
      mmddyyyyToDay("mm-dd-yyyy") shouldEqual "dd"
      mmddyyyyToMonth("mm-dd-yyyy") shouldEqual "mm"
      mmddyyyyToYear("mm-dd-yyyy") shouldEqual "yyyy"
    }
  }

}
