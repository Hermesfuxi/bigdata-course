package bigdata.hermesfuxi.spark.sql.shop

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object DSLShopIncomeDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val dataFrame: DataFrame = session.read
      .option("header", "true")
      .csv("data/shop.csv")

    import org.apache.spark.sql.functions._
    import session.implicits._
    dataFrame.select(
      'sid,
      substring('dt, 0, 7) as("mth"),
      'money,
    )
      .groupBy("sid","mth").agg(
      sum('money) as("sum_money"),
    )
      .select(
        'sid,
        'mth,
        'sum_money,
        sum('sum_money) over (Window.partitionBy("sid") orderBy("mth") rowsBetween(Window.unboundedPreceding, Window.currentRow)) as("total_money"),
      )
      .show()

    session.stop()
    session.close()
  }
}
