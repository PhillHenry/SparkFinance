package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.rdd.RDD

object StockSVM {

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val context         = getSparkContext(config)
      val all             = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
      val aggregated      = aggregate(all, isNotMeta, dayTickerToPriceVolume)
      val dependentTic    = config.tickers.head
      val indepdenentTics = config.tickers.drop(1)

      var joined: RDD[(TickerDate, Array[Double])] = null
    })
  }

}
