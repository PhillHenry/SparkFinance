package com.henryp.sparkfinance

import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object sparkjobs extends Logging {

  type TickerDate = String

  def aggregate[T: ClassTag](all:        RDD[(String, String)],
                             isNotMeta:  String => Boolean,
                             toDomain:   String => T ): RDD[T] = {
    def toRDD(ticker: String, text: String): TraversableOnce[T] = {
      val lines = text.lines.filter(isNotMeta(_))
      lines.map { case (line) => toDomain(line) }
    }
    all.flatMap { case(ticker, text) => toRDD(ticker, text) }
  }

  def dateAndPriceFor(raw:                RDD[String],
                      isNotMeta:          String => Boolean,
                      lineToDateAndPrice: String => (TickerDate, Double)): RDD[(TickerDate, Double)] =
    raw.filter(isNotMeta(_)).map(lineToDateAndPrice(_))

  def pearsonCorrelationValue[K](keyVal1: RDD[(K, Double)], keyVal2: RDD[(K, Double)]): Double = {
    val joined: RDD[(K, (Double, Double))] = keyVal1.join(keyVal2)

    val series1  = joined map { case(key, forKey) => forKey._1 }
    val series2  = joined map { case(key, forKey) => forKey._2 }

    val algorithm = "pearson"
    Statistics.corr(series1, series2, algorithm)
  }

}
