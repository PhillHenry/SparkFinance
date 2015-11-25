package com.henryp.sparkfinance

import com.henryp.sparkfinance.feeds._
import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object sparkjobs extends Logging {

  /**
   * @param all An RDD of key to content. Typically, filename to file content if loaded via SparkContext.wholeTextFiles.
   */
  def aggregate[T: ClassTag](all:        RDD[(String, String)],
                             isNotMeta:  String => Boolean,
                             toDomain:   (String, String) => T ): RDD[T] = {
    def toRDD(ticker: String, text: String): TraversableOnce[T] = {
      val lines = text.lines.filter(isNotMeta(_))
      lines.map { case (line) => toDomain(ticker, line) }
    }
    all.flatMap { case(ticker, text) => toRDD(ticker, text) }
  }

  def data[T: ClassTag](raw:        RDD[String],
                        isNotMeta:  String => Boolean,
                        toDomain:   String => T): RDD[T] =
    raw.filter(isNotMeta(_)).map(toDomain(_))

  def pearsonCorrelationValue[K: ClassTag](keyVal1: RDD[(K, Double)], keyVal2: RDD[(K, Double)]): Double = {
    val joined: RDD[(K, (Double, Double))] = keyVal1.join(keyVal2)

    val series1  = joined map { case(key, forKey) => forKey._1 }
    val series2  = joined map { case(key, forKey) => forKey._2 }

    val algorithm = "pearson"
    Statistics.corr(series1, series2, algorithm)
  }

  def seriesFor[D <: Tuple2[(U, String), Double], U: ClassTag](aggregated:  RDD[D],
                                                               ticker:      String,
                                                               toDatePrice: (D) => (U, Double)): RDD[(U, Double)]
    = aggregated.filter(matchesTicker[D](ticker, _)).map(toDatePrice)


  def joinByDate[T: ClassTag](tickers:    Seq[String],
                              domain:     RDD[((T, String), Double)],
                              toFeature:  (Tuple2[(T, String), Double] => (T, Double))): RDD[(T, Seq[Double])] = {
    val dependent = seriesFor(domain, tickers.head, toFeature)
    var joined    = dependent.map(kv => (kv._1, Seq(kv._2)))

    for (independentTicker <- tickers.tail) {
      val independent = seriesFor(domain, independentTicker, toFeature)
      joined = joined.join(independent).map(kv => (kv._1, kv._2._1 :+ kv._2._2))
    }

    joined
  }

}
