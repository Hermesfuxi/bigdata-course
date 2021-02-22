package bigdata.hermesfuxi.spark.sql.shop

import org.apache.spark.sql.SparkSession

object SQLShopIncomeDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val dataFrame = session.read
      .option("header", "true")
      .csv("data/shop.csv")
    dataFrame.createTempView("v_shop")
    val res = session.sql(
      """
        |select
        |    sid,
        |    mth,
        |    mth_sum,
        |    sum(mth_sum) over (partition by sid order by mth rows between unbounded preceding and current row ) total_sum
        |from (
        |         select
        |             sid,
        |             substr(dt, 0, 7) mth,
        |             sum(money) mth_sum
        |         from
        |             v_shop
        |         group by sid, substr(dt, 0, 7)
        |     ) b;
        |
        |""".stripMargin)

    res.show()
    res.printSchema()

    session.stop()
    session.close()
  }
}
