package com.henryp.sparkfinance.config

import org.apache.spark.{SparkConf, SparkContext}

object Spark {

  val localMaster = "local[*]"

  def sparkContext(master: String = localMaster): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("ftse")
    SparkContext.getOrCreate(conf)
  }
}
