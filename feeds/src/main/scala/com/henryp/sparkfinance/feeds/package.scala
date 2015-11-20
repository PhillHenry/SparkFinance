package com.henryp.sparkfinance

import scala.reflect.ClassTag

package object feeds {

  def matchesTicker[T <: Tuple2[(U, String), Double], U: ClassTag](ticker: String, tuple: T): Boolean = tuple._1._2.contains(ticker) // TODO remove the directory

}
