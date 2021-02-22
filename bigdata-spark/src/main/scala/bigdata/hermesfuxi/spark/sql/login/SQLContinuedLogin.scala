package bigdata.hermesfuxi.spark.sql.login

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object SQLContinuedLogin {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = sparkSession.read
      .schema(StructType(Array(
        StructField("name", StringType),
        StructField("login_date", DateType),
      )))
      .csv("data/data1.txt")
    df.createTempView("v_data")
    sparkSession.sql(
      s"""
         |select
         |    name, min(base_date) startTime, max(base_date) endTime, count(1) times
         |from (
         |     select
         |         name,
         |         login_date,
         |         date_sub(login_date, row_number() over (partition by name order by login_date)) base_date
         |     from
         |         v_data
         |) a
         |group by name, base_date
         |having times >= 3
         |
         |""".stripMargin).show()

    sparkSession.stop()
    sparkSession.close()
  }

}
