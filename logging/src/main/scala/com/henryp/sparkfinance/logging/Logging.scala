package com.henryp.sparkfinance.logging

trait Logging {

  def debug(msg: => String): Unit = println(msg)

  def error(msg: => String): Unit = println(msg) // do some proper logging
}
