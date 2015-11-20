package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._

object StockSVM {

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val context         = getSparkContext(config)
      val all             = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
      val dependentTic    = config.tickers.head
      val indepdenentTics = config.tickers.drop(1)
      val aggregated      = aggregate(all, isNotMeta, dayTickerToPrice[Int])

      joinByDate(config.tickers, aggregated, asDateToPrice[Int], dayTickerToPrice[Int])
    })
  }

}
