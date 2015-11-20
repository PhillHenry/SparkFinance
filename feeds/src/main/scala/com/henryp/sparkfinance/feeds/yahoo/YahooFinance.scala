package com.henryp.sparkfinance.feeds.yahoo

import java.io.{FileWriter, PrintWriter}

import com.henryp.sparkfinance.feeds.yahoo.YahooFinance.{parseEndDate, parseStartDate}

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Try}

class YahooDataExtractor(config: YahooFinanceConfig) {

  def readAllTickerData(): Unit = {
    val content: BufferedSource = Source.fromFile(config.tickerFile)
    content.getLines().foreach(downloadHistoricalData(_))
    content.close()
  }

  def downloadHistoricalData(ticker: String): Unit = {
    val writer    = new FileWriter(s"${config.outputDir}/$ticker")
    val bufWriter = new PrintWriter(writer)
    val tried     = Try(hitPage(ticker, handleLine(bufWriter)))

    tried match {
      case Failure(x) => println(ticker)
      case _ =>
    }

    writer.close()
  }

  def handleLine(writer: PrintWriter): String => Unit = writer.println(_)

  def hitPage(ticker: String, lineHandler: String => Unit): Unit = {
    val startDate = parseStartDate(config.startMMDDYYYY)
    val endDate   = parseEndDate(config.endMMDDYYYY)
    val content   = Source.fromURL(s"http://ichart.finance.yahoo.com/table.csv?s=$ticker&$startDate&$endDate&g=d&ignore=.csv")
    val iterator  = content.getLines()
    iterator.foreach(x => lineHandler(x))
    content.close()
  }
}

case class YahooFinanceConfig(tickerFile: String    = "tickers.tzt",
                              startMMDDYYYY: String = "07-19-2007",
                              endMMDDYYYY: String   = "07-19-2015",
                              outputDir: String     = "/tmp")

object YahooFinance {

  import com.henryp.sparkfinance.feeds.DateParsing._

  def parseStartDate(date: String): String = s"a=${mmddyyyyToMonth(date)}&b=${mmddyyyyToDay(date)}&c=${mmddyyyyToYear(date)}"
  def parseEndDate(date: String):   String = s"d=${mmddyyyyToMonth(date)}&e=${mmddyyyyToDay(date)}&f=${mmddyyyyToYear(date)}"

  def parseArgs(args: Array[String]): Option[YahooFinanceConfig] = {
    val parser = new scopt.OptionParser[YahooFinanceConfig]("Yahoo data extractor") {
      opt[String]('d', "directory")   action { case(value, config) => config.copy(outputDir = value) } text "data directory"
      opt[String]('s', "start")       action { case(value, config) => config.copy(startMMDDYYYY = value) } text "start date MM-DD-YYYY"
      opt[String]('e', "end")         action { case(value, config) => config.copy(endMMDDYYYY = value) } text "end date MM-DD-YYYY"
      opt[String]('f', "ticker file") action { case(value, config) => config.copy(tickerFile = value) } text "file with ticker symbols to download (1 ticker per line)"
    }
    parser.parse(args, YahooFinanceConfig())
  }

  /**
   * TODO use Scopt so we can make this CLI more versatile
   *
   * @param args a single arguments for where the tickers.txt file lives and also where files are output
   */
  def main(args: Array[String]): Unit = {
    parseArgs(args).orElse {
      None
    } foreach { config =>
      val app = new YahooDataExtractor(config)
      app.readAllTickerData()
      println("finished")
    }
  }

}
