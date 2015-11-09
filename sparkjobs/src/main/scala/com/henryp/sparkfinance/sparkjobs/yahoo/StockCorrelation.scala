package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.config.Spark
import com.henryp.sparkfinance.feeds.yahoo._
import com.henryp.sparkfinance.logging.Logging
import com.henryp.sparkfinance.sparkjobs._
import org.apache.spark.SparkContext

import scala.annotation.tailrec

case class StockCorrelationConfig(directory: String     = "./target",
                                  sparkUrl: String      = Spark.localMaster,
                                  tickers: Seq[String]  = List[String](),
                                  jars: Seq[String]     = List[String]() )

/**
 * Run with arguments like:
 *
 * -d /home/henryp/Documents/Finance/Historical/
 * -t HSBA,BARC
 * -s spark://192.168.1.9:7077
 * -j /home/henryp/Code/Scala/MyCode/SparkFinance/sparkjobs/target/sparkjobs-1.0-SNAPSHOT.jar,/home/henryp/Code/Scala/MyCode/SparkFinance/logging/target/logging-1.0-SNAPSHOT.jar,/home/henryp/Code/Scala/MyCode/SparkFinance/feeds/target/feeds-1.0-SNAPSHOT.jar
 */
object StockCorrelation extends Logging {

  def parseArgs(args: Array[String]): Option[StockCorrelationConfig] = {
    val parser = new scopt.OptionParser[StockCorrelationConfig]("StockCorrelation") {
      opt[String]('d', "directory") action { case(value, config) => config.copy(directory = value) } text "data directory"
      opt[String]('s', "spark") action { case(value, config) => config.copy(sparkUrl = value) } text "spark URL"
      opt[Seq[String]]('t', "tickers") valueName "<ticker>,<ticker>..."  action { (value, config) =>
        config.copy(tickers = value)
      } text "tickers"
      opt[Seq[String]]('j', "jars") valueName "<jar1>,<jar2>..."  action { (value, config) =>
        config.copy(jars = value)
      } text "jars"
    }
    parser.parse(args, StockCorrelationConfig())
  }

  def main(args: Array[String]): Unit = {
    val configOption = parseArgs(args)
    configOption.orElse {
      error("invalid arguments: " + args.mkString(","))
      None
    } foreach { config =>
      val comparisons = doCorrelations(config, {context =>
        info("Finished. Press any key to end app")
        Console.in.read
        context.stop()
      })
      info("Finished processing")
      comparisons foreach(x => info(x.toString()))
    }
  }

  def comparisonPairs[T](tickers: Seq[T]): Seq[(T, T)] = {
    @tailrec
    def allPairs[T](toProcess: List[T], already: Seq[(T, T)]): Seq[(T, T)] = {
      toProcess match {
        case Nil      => already
        case x :: xs  => allPairs(xs,  already ++ xs.map(other => (x, other)))
      }
    }
    allPairs(tickers.toList, Seq())
  }

  def doCorrelations(config: StockCorrelationConfig, onFinished: SparkContext => Unit): Seq[(String, String, Double)] = {
    val context       = Spark.sparkContext(config.sparkUrl)
    config.jars.foreach{ jar =>
      debug(s"Adding JAR $jar")
      context.addJar(jar)
    }
    val pairs         = comparisonPairs(config.tickers)
    debug(s"Comparing: ${pairs.mkString(",")}")
    val all           = context.wholeTextFiles(config.directory)
    val aggregated    = aggregate(all, isNotMeta, dateTickerToPrice)
    val pairsCorr     = pairs map { case(t1, t2) =>
      debug(s"processing $t1 and $t2")
      val series1 = aggregated.filter(matchesTicker(t1, _)).map(asDateToPrice)
      val series2 = aggregated.filter(matchesTicker(t2, _)).map(asDateToPrice)
      (t1, t2, pearsonCorrelationValue(series1, series2))
    }
    onFinished(context)
    pairsCorr
  }

}
