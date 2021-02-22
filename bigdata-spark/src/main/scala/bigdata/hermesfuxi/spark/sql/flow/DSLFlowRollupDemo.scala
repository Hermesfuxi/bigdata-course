package bigdata.hermesfuxi.spark.sql.flow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object DSLFlowRollupDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    val dataFrame = session.read
      .schema(StructType(Array(
        StructField("uid", StringType),
        StructField("start_time", StringType),
        StructField("end_time", StringType),
        StructField("flow", IntegerType),
      )))
      .csv("data/flow.txt")
    //    dataFrame.show()
    //    dataFrame.printSchema()

    import session.implicits._
    import org.apache.spark.sql.functions._

    dataFrame.select(
      'uid, 'start_time, 'end_time, 'flow,
      expr("lag(end_time, 1, start_time)") over (Window.partitionBy('uid) orderBy("start_time")) as("old_end_time")
    )
      .select(
        'uid, 'start_time, 'end_time, 'flow,
        'old_end_time,
        expr("if((to_unix_timestamp(start_time) - to_unix_timestamp(old_end_time)) > (10 * 60), 1, 0) result")
      )
      .select(
        'uid, 'start_time, 'end_time, 'flow,
        sum('result)
      )
      .show()

    session.stop()
    session.close()
  }
}
