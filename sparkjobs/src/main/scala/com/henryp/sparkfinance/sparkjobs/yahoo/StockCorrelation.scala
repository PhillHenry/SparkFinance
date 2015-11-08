package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.config.Spark

case class StockCorrelationConfig(directory: String = "./target",
                                  sparkUrl: String = Spark.localMaster,
                                  tickers: Seq[String] = List[String]())

object StockCorrelation {

  def parseArgs(args: Array[String]): Option[StockCorrelationConfig] = {
    val parser = new scopt.OptionParser[StockCorrelationConfig]("StockCorrelation") {
      opt[String]('d', "directory") action { case(value, config) => config.copy(directory = value) } text "data directory"
      opt[String]('s', "spark") action { case(value, config) => config.copy(sparkUrl = value) } text "spark URL"
      opt[Seq[String]]('t', "tickers") valueName "<ticker>,<ticker>..."  action { (value, config) =>
        config.copy(tickers = value)
      } text "tickers"
    }
    parser.parse(args, StockCorrelationConfig())
  }

  def main(args: Array[String]): Unit = {

  }

}
