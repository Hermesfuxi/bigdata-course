package bigdata.hermesfuxi.spark.streaming.windows

import bigdata.hermesfuxi.spark.utils.SparkUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true, this.getClass.getSimpleName)
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
