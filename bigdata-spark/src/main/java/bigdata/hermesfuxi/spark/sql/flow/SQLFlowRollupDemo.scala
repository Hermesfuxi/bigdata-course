package bigdata.hermesfuxi.spark.sql.flow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SQLFlowRollupDemo {
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

    dataFrame.createTempView("tb_flow")

    session.sql(
      """
        |select uid, min(start_time) start_time, max(end_time) end_time, sum(flow) sum_flow
        |from (
        |         select uid, start_time, end_time, flow,
        |                sum(result) over (partition by uid order by start_time ) sum_result
        |         from (
        |                  select uid, start_time, end_time, flow, old_end_time,
        |                         if((to_unix_timestamp(start_time) - to_unix_timestamp(old_end_time)) > 10 * 60, 1, 0) result
        |
        |                  from (
        |                           select uid, start_time, end_time, flow,
        |                                  lag(end_time, 1, start_time) over (partition by uid order by uid, start_time) old_end_time
        |                           from tb_flow
        |                   ) a
        |          ) b
        | )c
        |group by uid, sum_result
        |
        |
        |""".stripMargin).show()


    session.stop()
    session.close()

  }
}
