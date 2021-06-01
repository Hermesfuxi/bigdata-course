package bigdata.hermesfuxi.flink.tablesql.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, tableConversions}
import org.apache.flink.types.Row

/**
  * 批处理 Flink SQL Demo
  */
object BatchSQLDemo {

  case class Order(userId: Int, name: String, time: String, money: Double)

  def main(args: Array[String]): Unit = {
    //1.创建flink的批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.创建table的运行环境
    val tableEnv = BatchTableEnvironment.create(env)

    //3.从本地内存创建订单集合数据,并且封装成样例类
    val orderDataSet = env.fromCollection(
      List(
        Order(1, "zhangsan", "2018-10-20 15:30", 358.5),
        Order(2, "zhangsan", "2018-10-20 16:30", 131.5),
        Order(3, "lisi", "2018-10-20 16:30", 127.5),
        Order(4, "lisi", "2018-10-20 16:30", 328.5),
        Order(5, "lisi", "2018-10-20 16:30", 432.5),
        Order(6, "zhaoliu", "2018-10-20 22:30", 451.0),
        Order(7, "zhaoliu", "2018-10-20 22:30", 362.0),
        Order(8, "zhaoliu", "2018-10-20 22:30", 364.0),
        Order(9, "zhaoliu", "2018-10-20 22:30", 341.0)
      )
    )

    //4.将数据注册成表
    tableEnv.registerDataSet("order_table", orderDataSet, 'userId, 'name, 'time, 'money)
    //
    //    //5.编写sql  最后使用sqlQuery来执行
    val sql =
    """
      |select name,
      |       max(money) as maxMoney,
      |       min(money) as minMoney,
      |       count(1) as orderNum
      |from order_table
      |group by name
      |
    """.stripMargin

    val resultTable = tableEnv.sqlQuery(sql)

    resultTable.printSchema()

    //将读取出来的数据转换为dataSet
    resultTable.toDataSet[Row].print()

    // 批处理不需要此语句
//    env.execute()
  }
}
