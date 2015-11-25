package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object StockSVM {

  val parser: (String, String) => DateTickerPrice[Int] = dayTickerToPriceChange[Int]

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val context         = getSparkContext(config)
      svmForPriceChanges(config, context)
    })
  }

  def svmForPriceChanges(config: StockCorrelationConfig, context: SparkContext): Double = {
    val dependentTic    = config.tickers.head
    val independentTics = config.tickers.drop(1)
    val all             = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
    val aggregated      = aggregate(all, isNotMeta, parser)
    useSVM(dependentTic, independentTics, aggregated)
  }

  def useSVM(dependentTic: String, independentTics: Seq[String], aggregated: RDD[((Int, String), Double)]): Double = {
    val dependentByDate   = seriesFor(aggregated, dependentTic, asDateToDouble[Int])
    val independentByDate = joinByDate(independentTics, aggregated, asDateToDouble[Int])
    val timeShiftedSeries = shiftIndex1Backward(dependentByDate)
    val model             = buildModel(timeShiftedSeries, independentByDate)
    val purchases         = dependentByDate.join(independentByDate).map(advisePurchaseBasedOn(model))

    val total = purchases.map({ case(date, buy) =>
      info(s"$date : $buy")
      buy
    }).sum()

    info(s"total = $total")
    total
  }

  def advisePurchaseBasedOn(model: SVMModel): ((Int, (Double, Seq[Double]))) => (Int, Double) = { case(date, changeToFeature) =>
    val change    = changeToFeature._1
    val features  = changeToFeature._2
    val isBuy     = model.predict(Vectors.dense(features.toArray))
    val total     = if (isBuy == 1d) change else 0
    info(s"$date : buy? ${isBuy == 1d}, delta = $change, subtotal = $total")
    (date, total)
  }

  def buildModel(timeShiftedSeries: RDD[(Int, Double)], independentByDate: RDD[(Int, Seq[Double])]): SVMModel = {
    val rdd       = upOrDown(timeShiftedSeries).join(independentByDate)
    val maxDate   = independentByDate.map(kv => kv._1).max()
    val splitDate = maxDate - 30

    val training  = rdd.filter(kv => kv._1 < splitDate).map(toTargetFeatures).cache()
    val test      = rdd.filter(kv => kv._1 >= splitDate).map(toTargetFeatures)

    train(training, test)
  }

  def toTargetFeatures: ((Int, (Double, Seq[Double]))) => LabeledPoint = { case(date, targetAndFeatures) =>
    LabeledPoint(targetAndFeatures._1, Vectors.dense(targetAndFeatures._2.toArray))
  }

  def train(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): SVMModel = {
    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    //    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    info("Area under ROC = " + auROC)

    scoreAndLabels.collect.foreach { case (score, label) =>
      println(s"score = $score, label = $label")
    }

    model
  }

  def shiftIndex1Backward(series: RDD[(Int, Double)]): RDD[(Int, Double)] = {
    series.map(kv => (kv._1 - 1, kv._2))
  }

  def upOrDown[T](series: RDD[(T, Double)]): RDD[(T, Double)] = {
    series.map(kv => (kv._1, {if (kv._2 > 0) 1d else 0d }))
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
