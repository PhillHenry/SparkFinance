package com.henryp.sparkfinance.sparkjobs.yahoo

import com.henryp.sparkfinance.sparkjobs.yahoo.StockSVM._
import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.ClassTag

class StockSVMSpec extends WordSpec with Matchers with SparkFixture {

  import StockSVMSpec._

  "negative and zero -> 0, positive -> 1" should {
    "be the mapping" in {
      val rdd = toRDD(-3 to 3)
      val buy = upOrDown(rdd)
      valuesOf(buy) shouldEqual Seq(0,0,0,0, 1,1,1)
    }
  }

  def toRDD(seq: Seq[Int]): RDD[(Int, Double)] =  context.parallelize(seq.map(x => (x, x.toDouble)))
}

object StockSVMSpec {
  def valuesOf[U, T: ClassTag](rdd: RDD[(U, T)]): Seq[T] = {
      rdd.map(kv => kv._2).collect()
  }
}
