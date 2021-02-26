package bigdata.hermesfuxi.spark.streaming.windows

import bigdata.hermesfuxi.spark.utils.SparkUtils
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getContext(true, this.getClass.getSimpleName)
    sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop-master", 8888)
    lines.foreachRDD(rdd=>{println(rdd.collect().toBuffer)})

    // window 仅仅只是获取数据范围变大了
    // val windowed = lines.window(Seconds(10), Seconds(10)) 效果和没用一样
    val windowed = lines.window(Seconds(20), Seconds(20))
    windowed.foreachRDD(rdd=>println("windowed: " + rdd.collect().toBuffer))

    lines.countByValueAndWindow(Seconds(20), Seconds(20)).foreachRDD(rdd=>println("countByValueAndWindow: " + rdd.collect().toBuffer))

    lines.countByWindow(Seconds(20), Seconds(20)).foreachRDD(rdd=>println("countByWindow: " + rdd.collect().toBuffer))

    lines.reduceByWindow(_+_,Seconds(20), Seconds(20))

    val wordAndCount: DStream[(String, Int)] = lines.map(line => {
      (line, 1)
    })
    val result: DStream[(String, Int)] = wordAndCount.reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Seconds(20), Seconds(20))
    result.foreachRDD(rdd=>println("reduceByKeyAndWindow: " + rdd.collect().toBuffer))

    /**
     * ArrayBuffer(q, r, s, t, u, v, w, x)
     * windowed: ArrayBuffer(j, k, l, m, n, o, p, q, r, s, t, u, v, w, x)
     * countByValueAndWindow: ArrayBuffer((p,1), (q,1), (r,1), (s,1), (t,1), (u,1), (v,1), (w,1), (x,1), (j,1), (k,1), (l,1), (m,1), (n,1), (o,1))
     * countByWindow: ArrayBuffer(15)
     * reduceByKeyAndWindow: ArrayBuffer((p,1), (q,1), (r,1), (s,1), (t,1), (u,1), (v,1), (w,1), (x,1), (j,1), (k,1), (l,1), (m,1), (n,1), (o,1))
     */

    streamingContext.checkpoint("./.ck/ck1")
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
