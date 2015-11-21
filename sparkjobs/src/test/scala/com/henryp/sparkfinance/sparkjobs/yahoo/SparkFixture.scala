package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.logging.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}


trait SparkFixture extends BeforeAndAfterAll with Logging { this:Suite =>
  val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("ftse")
  val context = SparkContext.getOrCreate(conf)

  override protected def afterAll(): Unit = {
    info("Stopping Spark context")
    context.stop()
  }
}
