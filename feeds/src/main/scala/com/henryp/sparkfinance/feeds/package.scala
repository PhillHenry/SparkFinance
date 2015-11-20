package com.henryp.sparkfinance

package object feeds {

  def matchesTicker[T <: Tuple2[(Any, String), Double]](ticker: String, tuple: T): Boolean = tuple._1._2.contains(ticker) // TODO remove the directory

}
