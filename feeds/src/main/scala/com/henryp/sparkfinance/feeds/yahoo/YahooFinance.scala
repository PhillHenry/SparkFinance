package com.henryp.sparkfinance.feeds.yahoo

import java.io.{FileWriter, PrintWriter}

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Try}

class YahooDataExtractor(directory: String) {

  def readAllTickerData(): Unit = {
    val content: BufferedSource = Source.fromFile(s"$directory/tickers.txt")
    content.getLines().foreach(getHistoricalData(_))
    content.close()
  }

  def getHistoricalData(ticker: String): Unit = {
    val writer = new FileWriter(s"$directory/$ticker")
    val bufWriter = new PrintWriter(writer)
    val tried = Try(hitPage(ticker, handleLine(bufWriter)))

    tried match {
      case Failure(x) => println(ticker)
      case _ =>
    }

    writer.close()
  }

  def handleLine(writer: PrintWriter): String => Unit = writer.println(_)

  def hitPage(ticker: String, lineHandler: String => Unit): Unit = {
    // TODO make the dates configurable
    val content: BufferedSource = Source.fromURL(s"http://ichart.finance.yahoo.com/table.csv?s=$ticker&a=07&b=19&c=2007&d=07&e=19&f=2015&g=d&ignore=.csv")
    val iterator = content.getLines()
    iterator.foreach(x => lineHandler(x))
    content.close()
  }
}

object YahooFinance {

  /**
   * TODO use Scopt so we can make this CLI more versatile
   *
   * @param args a single arguments for where the tickers.txt file lives and also where files are output
   */
  def main(args: Array[String]): Unit = {
    val app = new YahooDataExtractor(args(0))
    app.readAllTickerData()
    println("finished")
  }

}
