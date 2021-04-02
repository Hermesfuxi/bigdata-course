package bigdata.hermesfuxi.spark.structured

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

object StructuredStreamingDemo {
  def main(args: Array[String]): Unit = {
    //1.构建SparkSession
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //2.创建输入流-readStream
    var lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop-slave1")
      .option("port", 8888)
      .load()

    //3.对dataframe实现转换
    var wordCounts = lines.as[String].flatMap(_.split("\\s+")).groupBy("value").count()


    //4.构建query 输出
    val query = wordCounts.writeStream
//      .outputMode(OutputMode.Update()) //有状态持续计算 Complete| Update
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    //5.等待流结束
    query.awaitTermination()

    spark.close();
  }

}
