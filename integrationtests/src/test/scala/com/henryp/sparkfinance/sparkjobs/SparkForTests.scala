package com.henryp.sparkfinance.sparkjobs

import com.henryp.sparkfinance.config.Spark
import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.rdd.RDD

object SparkForTests extends Logging {

  info("===================== Starting Spark Context")
  val context       = Spark.sparkContext()

  def wholeTextFiles(dataDirectory: String): RDD[(String, String)]  = context.wholeTextFiles(dataDirectory)

  def stop(): Unit = {
    info("===================== Stopping Spark Context")
    context.stop()
  }
}
