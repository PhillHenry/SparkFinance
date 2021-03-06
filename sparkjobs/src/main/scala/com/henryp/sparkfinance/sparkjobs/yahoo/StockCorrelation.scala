package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.logging.Logging
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Run with arguments like:
 *
 * -d /home/henryp/Documents/Finance/Historical/
 * -t HSBA,BARC
 * -s spark://192.168.1.9:7077
 * -j /home/henryp/Code/Scala/MyCode/SparkFinance/sparkjobs/target/sparkjobs-1.0-SNAPSHOT.jar
 */
object StockCorrelation extends Logging {

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val comparisons = doCorrelations(config, {context =>
        waitForKeyThenStop(context)
      })
      info("Finished processing")
      comparisons foreach(x => info(x.toString()))
    })
  }

  def comparisonPairs[T](tickers: Seq[T]): Seq[(T, T)] = {
    @tailrec
    def allPairs[U](toProcess: List[U], already: Seq[(U, U)]): Seq[(U, U)] = {
      toProcess match {
        case Nil      => already
        case x :: xs  => allPairs(xs,  already ++ xs.map(other => (x, other)))
      }
    }
    allPairs(tickers.toList, Seq())
  }

  def doCorrelations(config: StockCorrelationConfig, onFinished: SparkContext => Unit): Seq[(String, String, Double)] = {
    val context       = getSparkContext(config)
    val all           = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
    val aggregated    = aggregate(all, isNotMeta, dateTickerToPrice[TickerDate])
    val pairs         = comparisonPairs(config.tickers)
    val pairsCorr     = findPearsonCorrelation(pairs, aggregated)
    onFinished(context)
    pairsCorr
  }

  def findPearsonCorrelation[T: ClassTag](pairs: Seq[(String, String)], aggregated: RDD[DateTickerPrice[T]]): Seq[(String, String, Double)] = {
    debug(s"Comparing: ${pairs.mkString(",")}")
    val pairsCorr = pairs map { case (ticker1, ticker2) =>
      debug(s"processing $ticker1 and $ticker2")
      val series1 = seriesFor(aggregated, ticker1, asDateToDouble[T])
      val series2 = seriesFor(aggregated, ticker2, asDateToDouble[T])
      (ticker1, ticker2, pearsonCorrelationValue(series1, series2))
    }
    pairsCorr
  }

}
