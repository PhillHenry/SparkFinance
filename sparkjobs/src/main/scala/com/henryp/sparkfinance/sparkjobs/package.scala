package com.henryp.sparkfinance

import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

package object sparkjobs extends Logging {

  type TickerDate = String

  def dateAndPriceFor(raw:                RDD[String],
                      isNotMeta:          String => Boolean,
                      lineToDateAndPrice: String => (TickerDate, Double)): RDD[(TickerDate, Double)] =
    raw.filter(isNotMeta(_)).map(lineToDateAndPrice(_))

  def pearsonCorrelationValue(datePrice1: RDD[(TickerDate, Double)], datePrice2: RDD[(TickerDate, Double)]): Double = {
    val joined: RDD[(TickerDate, (Double, Double))] = datePrice1.join(datePrice2)

    val prices1  = joined map { case(date, prices) => prices._1 }
    val prices2  = joined map { case(date, prices) => prices._2 }

    val algorithm = "pearson"
    val correlation = Statistics.corr(prices1, prices2, algorithm)

    correlation
  }

}
