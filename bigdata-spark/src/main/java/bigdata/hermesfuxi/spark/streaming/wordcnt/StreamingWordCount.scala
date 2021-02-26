package bigdata.hermesfuxi.spark.streaming.wordcnt

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")

    // 创建一个（增强的SparkContext）即StreamingContext，并指定每个批次生成的时间周期
    //    val streamingContext = new StreamingContext(conf, Seconds(10))
    val sparkContext = new SparkContext(conf)
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(10000))

    streamingContext
    //创建抽象的数据集DStream（是对RDD的增强）
    //linux安装nc（yum install -y nc），并执行程序（nc -ls 8888）
    val inputLine = streamingContext.socketTextStream("hadoop-master", 8888)

    // 调用DStream的方法（Transformation）: 计算wordcount
    val wordAndOne = inputLine.flatMap(_.split("\\s+")).map((_, 1))

    // 一、不累计的当前行
    //    val result = wordAndOne.reduceByKey(_ + _)

    // 二、累计所有行
    streamingContext.checkpoint("./checkpoint")
    val result = wordAndOne.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      Some(seq.sum + option.getOrElse(0))
    })


    //调用Action
    result.print()

    //启动Application，让其一直运行
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
