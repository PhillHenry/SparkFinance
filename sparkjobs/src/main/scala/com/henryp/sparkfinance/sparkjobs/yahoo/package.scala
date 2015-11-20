package com.henryp.sparkfinance.sparkjobs

import com.henryp.sparkfinance.config.Spark
import org.apache.spark.SparkContext

package object yahoo {

  case class StockCorrelationConfig(directory: String     = "./target",
                                    sparkUrl: String      = Spark.localMaster,
                                    tickers: Seq[String]  = List[String](),
                                    jars: Seq[String]     = List[String](),
                                    numPartitions: Int    = 2)

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
      opt[Int]('p', "partitions") action { case(value, config) => config.copy(numPartitions = value) } text "number of partions"
    }
    parser.parse(args, StockCorrelationConfig())
  }

  def getSparkContext(config: StockCorrelationConfig): SparkContext = {
    val context = Spark.sparkContext(config.sparkUrl)
    config.jars.foreach { jar =>
      debug(s"Adding JAR $jar")
      context.addJar(jar)
    }
    context
  }

  def runWith(args: Array[String], toRun: (StockCorrelationConfig) => Unit): Unit = {
    val configOption = parseArgs(args)
    configOption.orElse {
      error("invalid arguments: " + args.mkString(","))
      None
    } foreach { config =>
      toRun(config)
    }
  }

}
