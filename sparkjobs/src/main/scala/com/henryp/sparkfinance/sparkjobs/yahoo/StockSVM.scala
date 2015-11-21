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
      val aggregated      = aggregate(all, isNotMeta, dayTickerToPrice[Int])

      val changes         = changesFor(seriesFor(aggregated, dependentTic, asDateToPrice[Int]))

      val independent     = joinByDate(indepdenentTics, aggregated, asDateToPrice[Int], dayTickerToPrice[Int])
    })
  }

  def shiftIndex1Backward(series: RDD[(Int, Double)]): RDD[(Int, Double)] = {
    series.map(kv => (kv._1 - 1, kv._2))
  }

  def upOrDown[T](series: RDD[(T, Double)]): RDD[(T, Int)] = {
    series.map(kv => (kv._1, {if (kv._2 > 0) 1 else 0 }))
  }

  def changesFor[T: Ordering: ClassTag](series: RDD[(T, Double)]): RDD[(T, Double)] = {
    var last = 0d

    series.sortByKey(numPartitions = 1).map { kv => // TODO numPartitions = 1 to make the sort not partitioned but is it slow...?
      val oldLast = last
      last = kv._2
      println(kv)
      (kv._1, kv._2 - oldLast)
    }
  }

}
