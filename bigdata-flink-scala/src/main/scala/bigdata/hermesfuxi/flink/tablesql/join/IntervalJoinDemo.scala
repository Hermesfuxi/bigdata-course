package bigdata.hermesfuxi.flink.tablesql.join

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

import scala.collection.mutable

/**
  * 在下单之后，在一个小时之内付款的数据才是有效数据，使用  JOIN INTERVAL  来实现
  */
object IntervalJoinDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(String, String, Timestamp)]
    ordersData.+=(("001", "iphone", new Timestamp(1545800002000L)))
    ordersData.+=(("002", "mac", new Timestamp(1545800003000L)))
    ordersData.+=(("003", "book", new Timestamp(1545800004000L)))
    ordersData.+=(("004", "cup", new Timestamp(1545800018000L)))

    // 构造付款表
    val paymentData = new mutable.MutableList[(String, String, Timestamp)]
    paymentData.+=(("001", "alipay", new Timestamp(1545803501000L)))
    paymentData.+=(("002", "card", new Timestamp(1545803602000L)))
    paymentData.+=(("003", "card", new Timestamp(1545803610000L)))
    paymentData.+=(("004", "alipay", new Timestamp(1545803611000L)))


    val ordersDataStream = env.fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[String, String]())
    val ordersTable = tableEnv.fromDataStream(ordersDataStream, 'orderId, 'productName, 'orderTime.rowtime)
    tableEnv.createTemporaryView("Orders", ordersTable)
//    ordersTable.toRetractStream[Row].print("Orders")


    val paymentDataStream = env.fromCollection(paymentData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[String, String]())
    val paymentDataTable = tableEnv.fromDataStream(paymentDataStream, 'orderId, 'payType, 'payTime.rowtime)
    tableEnv.createTemporaryView("Payment", paymentDataTable)
//    paymentDataTable.toRetractStream[Row].print("Payment")

    var sqlQuery: String =
      """
        |SELECT
        |  o.orderId,
        |  o.productName,
        |  p.payType,
        |  o.orderTime,
        |  cast(payTime as timestamp) as payTime
        |FROM
        |Orders AS o
        |JOIN Payment AS p
        |ON
        |  o.orderId = p.orderId AND
        |  p.payTime BETWEEN orderTime - INTERVAL '1' HOUR AND
        |  orderTime + INTERVAL '1' HOUR
        |""".stripMargin

    val resultTable: Table = tableEnv.sqlQuery(sqlQuery)

    resultTable.toRetractStream[Row].print("resultTable")

    env.execute()

  }

  class TimestampExtractor[T1, T2] extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
    override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
      element._3.getTime
    }
  }

}