package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._

object StockSVM {

  def main(args: Array[String]): Unit = {
    runWith(args, {config =>
      val context       = getSparkContext(config)
      val all           = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
      val aggregated    = aggregate(all, isNotMeta, dateTickerToPrice)
    })
  }

}
