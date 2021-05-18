package bigdata.hermesfuxi.flink.sql.windows

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

/**
 * 滑动窗口 sql demo
 */
object FlinkSQLSessionTime {
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val inputStream = env.readTextFile("data/sensor.txt")
    val dataStream = inputStream.map(data => {
      val fields = data.split(",")
      SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
    })
      // 新版
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading](){
            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000L
          })
      )
    // 老版
//          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//            override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
//          })

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    val ddlTable = sensorTable
      .window(Session withGap (10.seconds) on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count(), 'tw.`end`())

    /*** 其中涉及时间语义如下：
     * .window(Session withGap (10.seconds) on 'rowtime as 'w) （事件时间字段 rowtime）
     * .window(Session withGap (10.seconds) on 'proctime as 'w)（处理时间字段 proctime）
     * .window(Session withGap (10.rows) on 'proctime as 'w) (类似于计数窗口，按处理时间排序，10 行一组)
     */
    ddlTable.toAppendStream[Row].print("ddlTable")

    tableEnv.createTemporaryView("sensor", sensorTable)
    val sqlTable = tableEnv.sqlQuery(
      """
        |select
        |id,
        |count(id) ,
        |SESSION_END(ts,interval '10' second)
        |from sensor
        |group by
        |id,
        |SESSION(ts,interval '10' second)
        |""".stripMargin)

    sqlTable.toRetractStream[Row].print("sqlTable")

    env.execute()
  }
}
