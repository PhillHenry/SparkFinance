package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object StockSVM {

  val parser: (String, String) => DateTickerPrice[Int] = dayTickerToPriceChange[Int]

  def main(args: Array[String]): Unit = {
    runWith(args, { config =>
      val context         = getSparkContext(config)
      val total = svmForPriceChanges(config, context)
      println("PnL: " + total)
      waitForKeyThenStop(context)
    })
  }

  def svmForPriceChanges(config: StockCorrelationConfig, context: SparkContext): Double = {
    val dependentTic          = config.tickers.head
    val independentTics       = config.tickers.drop(1)
    val aggregated            = allData(config, context).cache()
    val (model, testingData)  = modelAndTestData(dependentTic, independentTics, aggregated)
    val advice                = testingData.map(advisedPurchaseBasedOn(model))
    val total                 = calcTotal(advice)

    val confidence            = areaUnderROC(model, testingData)
    info("Area under ROC = " + confidence)

    total
  }

  def allData(config: StockCorrelationConfig, context: SparkContext): RDD[((Int, String), Double)] = {
    val all         = context.wholeTextFiles(config.directory, minPartitions = config.numPartitions)
    val aggregated  = aggregate(all, isNotMeta, parser)
    aggregated
  }

  def areaUnderROC(model: SVMModel, testingData: RDD[(Int, (Double, Seq[Double]))]): Double = {
    model.clearThreshold()
    // Compute raw scores on the test set.
    val scoreAndLabels = testingData.map { case (date, targetAndFeature) =>
      val score = model.predict(featuresToVector(targetAndFeature._2))
      (score, targetAndFeature._1)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    metrics.areaUnderROC()
  }

  def modelAndTestData(dependentTic: String,
                       independentTics: Seq[String],
                       aggregated: RDD[((Int, String), Double)]): (SVMModel, RDD[(Int, (Double, Seq[Double]))]) = {
    info(s"dependent variables: $dependentTic, independent variables: $independentTics")
    val dependentByDate   = seriesFor(aggregated, dependentTic, asDateToDouble[Int]).cache()
    val independentByDate = joinByDate(independentTics, aggregated, asDateToDouble[Int]).cache()
    val timeShiftedSeries = shiftIndex1Backward(dependentByDate)

    // this throws a NPE if you don't have the same version of Scala in the workers as the driver
    val dates: RDD[Int]   = dependentByDate.map(_._1)
    val maxDate           = dates.max()

    val splitDate         = maxDate - 30

    val model             = buildModel(timeShiftedSeries, independentByDate, filterDateBefore(splitDate))
    val testingData       = dependentByDate.filter(filterDateOnOrAfter(splitDate)).join(independentByDate)
    (model, testingData)
  }

  def calcTotal(advice: RDD[(Int, Double)]): Double = {
    val total = advice.map({ case (date, buy) =>
      info(s"$date : $buy")
      buy
    }).sum()
    total
  }

  def filterDateBefore(date: Int): Product => Boolean = { tuple => tuple.productElement(0).asInstanceOf[Int] < date }
  def filterDateOnOrAfter(date: Int): Product => Boolean = { tuple => tuple.productElement(0).asInstanceOf[Int] >= date }

  def advisedPurchaseBasedOn(model: SVMModel): ((Int, (Double, Seq[Double]))) => (Int, Double) = { case(date, changeToFeature) =>
    val change    = changeToFeature._1
    val features  = changeToFeature._2
    val isBuy     = model.predict(featuresToVector(features))
    val total     = if (isBuy == 1d) change else 0
    info(s"$date : buy? ${isBuy == 1d}, delta = $change, subtotal = $total")
    (date, total)
  }

  def buildModel(timeShiftedSeries: RDD[(Int, Double)],
                 independentByDate: RDD[(Int, Seq[Double])],
                 filter: Product => Boolean): SVMModel = {
    val rdd       = upOrDown(timeShiftedSeries).join(independentByDate)
    val training  = rdd.filter(filter).map(toTargetFeatures).cache()

    train(training)
  }

  def toTargetFeatures: ((Int, (Double, Seq[Double]))) => LabeledPoint = { case(date, targetAndFeatures) =>
    LabeledPoint(targetAndFeatures._1, featuresToVector(targetAndFeatures._2))
  }

  def featuresToVector(features: Seq[Double]): Vector = Vectors.dense(features.toArray)

  def train(training: RDD[LabeledPoint]): SVMModel = {
    // Run training algorithm to build the model
    val numIterations = 100
    SVMWithSGD.train(training, numIterations)
  }

  def shiftIndex1Backward(series: RDD[(Int, Double)]): RDD[(Int, Double)] = {
    series.map(kv => (kv._1 - 1, kv._2))
  }

  def upOrDown[T](series: RDD[(T, Double)]): RDD[(T, Double)] = {
    series.map(kv => (kv._1, {if (kv._2 > 0) 1d else 0d }))
  }

}
