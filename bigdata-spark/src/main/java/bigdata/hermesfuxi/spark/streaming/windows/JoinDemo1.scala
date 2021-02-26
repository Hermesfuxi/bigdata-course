package bigdata.hermesfuxi.spark.streaming.windows

import bigdata.hermesfuxi.spark.utils.SparkUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object JoinDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true, this.getClass.getSimpleName)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    val lines1 = streamingContext.socketTextStream("hadoop-master", 8888)
    val lines2 = streamingContext.socketTextStream("hadoop-master", 9999)

    val wordAndCount = lines1.map(line => {
      val fields = line.split(",")
      val word = fields(0)
      val count = fields(1).toInt
      (word, count)
    })

    val wordAndChar = lines2.map(line => {
      val fields = line.split(",")
      val word = fields(0)
      val chr = fields(1)
      (word, chr)
    })

    val joined = wordAndCount.join(wordAndChar)

    joined.print()

    streamingContext.checkpoint("./ck2")
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
