package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object StockSVM {

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val context         = getSparkContext(config)
      val all             = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
      val dependentTic    = config.tickers.head
      val indepdenentTics = config.tickers.drop(1)

      joinByDate(dependentTic, indepdenentTics, all, dayTickerToPrice)
    })
  }

  def joinByDate[T: ClassTag](dependentTic:     String,
                              indepdenentTics:  Seq[String],
                              byFile:           RDD[(String, String)],
                              dayToFeature:     (String, String) => DateTickerPrice[T]): RDD[(T, Seq[Double])] = {
    val aggregated      = aggregate(byFile, isNotMeta, dayToFeature)
    val dependent       = seriesFor(aggregated, dependentTic, asDateToPrice[T] _)

    var joined: RDD[(T, Seq[Double])] = dependent.map(kv => (kv._1, Seq(kv._2)))

    for (independentTicker <- indepdenentTics) {
      val independent = seriesFor(aggregated, independentTicker, asDateToPrice[T] _)
      joined = joined.join(independent).map(kv => (kv._1, kv._2._1 :+ kv._2._2))
    }

    joined
  }

}
