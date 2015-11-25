package com.henryp.sparkfinance.feeds.yahoo

import java.io.{FileWriter, PrintWriter}

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Try}

class YahooDataExtractor(config: YahooFinanceConfig) {

  def readAllTickerData(): Unit = {
    val content: BufferedSource = Source.fromFile(config.tickerFile)
    content.getLines().foreach(downloadHistoricalData)
    content.close()
  }

  def downloadHistoricalData(ticker: String): Unit = {
    val writer    = new FileWriter(s"${config.outputDir}/$ticker")
    val bufWriter = new PrintWriter(writer)
    val tried     = Try(hitPage(ticker, handleLine(bufWriter)))

    tried match {
      case Failure(x) => println(ticker + " error = " + x.getMessage)
      case _ =>
    }

    writer.close()
  }

  def handleLine(writer: PrintWriter): (String, Option[String]) => Unit = { case (line, lastLine) =>
    writer.println(toLine(line, lastLine))
  }

  val toLine: ((String, Option[String]) => String) = { case (line, lastLine) =>
    lastLine.map ({ last =>
      val priceDiff = diff(closingPriceElement(line), closingPriceElement(last))
      line + "," + priceDiff
    }).orElse ({
      Some(line + ",0")
    }).get
  }

  def hitPage(ticker: String, lineHandler: (String, Option[String]) => Unit): Unit = {
    val startDate                 = config.startMMDDYYYY
    val endDate                   = config.endMMDDYYYY
    val url                       = s"http://ichart.finance.yahoo.com/table.csv?s=$ticker&$startDate&$endDate&g=d&ignore=.csv"
    val content                   = Source.fromURL(url)
    val iterator                  = content.getLines()
    var lastLine: Option[String]  = None
    println(url)
    iterator.foreach { line =>
      lineHandler(line, lastLine)
      if (isNotMeta(line)) lastLine = Some(line)
    }
    content.close()
  }
}

case class YahooFinanceConfig(tickerFile: String    = "tickers.txt",
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
