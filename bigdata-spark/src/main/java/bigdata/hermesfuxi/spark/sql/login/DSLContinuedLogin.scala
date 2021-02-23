package bigdata.hermesfuxi.spark.sql.login

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object DSLContinuedLogin {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = session.read
      .schema(StructType(Array(
        StructField("name", StringType),
        StructField("login_date", DateType),
      )))
      .csv("data/data1.txt").distinct()

    import org.apache.spark.sql.functions._
    import session.implicits._

    df.select(
      'name,
      'login_date,
      date_sub('login_date, row_number() over (Window.partitionBy("name") orderBy "login_date")).as("base_date")
    )
      .groupBy(
        "name", "base_date",
      )
      .agg(
        max('base_date).as("endTime"),
        min('base_date).as("startTime"),
        count("*").as("times"),
      )
      .where('times >= 3)
      .drop("base_date")
      .show()

    session.stop()
  }

}
