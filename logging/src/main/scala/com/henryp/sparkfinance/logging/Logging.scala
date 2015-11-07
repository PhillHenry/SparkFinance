package com.henryp.sparkfinance.logging

trait Logging {

  def debug(msg: => String): Unit = println(msg)

}
