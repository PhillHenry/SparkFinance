package com.henryp.sparkfinance.sparkjobs

import com.henryp.sparkfinance.config.Spark
import org.apache.spark.rdd.RDD

object SparkForTests {

  val context       = Spark.sparkContext()

  def wholeTextFiles(dataDirectory: String): RDD[(String, String)]  = context.wholeTextFiles(dataDirectory)

  def stop(): Unit = {
    context.stop()
  }
}
