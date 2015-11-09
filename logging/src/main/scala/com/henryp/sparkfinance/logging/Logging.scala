package com.henryp.sparkfinance.logging

trait Logging {

  def debug(msg: => String): Unit = println(msg)

  def error(msg: => String): Unit = debug(msg) // do some proper logging

  def info(msg: => String): Unit = debug(msg) // TODO proper logging
}
