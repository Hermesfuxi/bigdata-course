package bigdata.hermesfuxi.spark.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  def getContext(isLocal: String): SparkContext = {
    getContext("true".equals(isLocal))
  }

  def getContext(isLocal: Boolean): SparkContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    if (isLocal) {
      sparkConf.setMaster("local[*]")
    }
    new SparkContext(sparkConf)
  }

}
