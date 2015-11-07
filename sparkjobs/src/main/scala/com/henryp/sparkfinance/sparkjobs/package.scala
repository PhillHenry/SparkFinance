package com.henryp.sparkfinance

import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

package object sparkjobs extends Logging {

  type TickerDate = String

  def dateAndClosePriceFor(raw: RDD[String],
                           isNotMeta: String => Boolean,
                           lineToDateAndClosePrice: String => (TickerDate, Double)): RDD[(String, Double)] =
    raw.filter(isNotMeta(_)).map(lineToDateAndClosePrice(_))

  def correlationValue(ticker1: String, ticker2: String, context: SparkContext): Double = {
    val datePrice1: RDD[(TickerDate, Double)] = ??? // dateAndClosePriceFor(ticker1, context)
    val datePrice2: RDD[(TickerDate, Double)] = ??? //dateAndClosePriceFor(ticker2, context)

    val joined: RDD[(TickerDate, (Double, Double))] = datePrice1.join(datePrice2)

    val dates = joined.map(x => x._1)
    debug("First date = " + dates.min())
    debug("last       = " + dates.max()) // dates.first() is not necessarily the min

    debug(s"Number of joined dates = ${joined.count()}")

    val prices1  = joined map { case(date, prices) => prices._1 }
    val prices2  = joined map { case(date, prices) => prices._2 }

    val algorithm = "pearson"
    val correlation = Statistics.corr(prices1, prices2, algorithm)

    debug(s"$algorithm correlation between $ticker1 and $ticker2 is $correlation")
    correlation
  }

}
