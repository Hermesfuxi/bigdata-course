package bigdata.hermesfuxi.spark.example.cube

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CubeDistinctAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val frame: DataFrame = spark.read
      .schema(StructType(Array(
        StructField("guid", StringType),
        StructField("province", StringType),
        StructField("city", StringType),
        StructField("region", StringType),
      )))
      .csv("data/test.txt")

    frame.createTempView("input")
    val result1 = spark.sql(
      """
        |select
        |    province,
        |    city,
        |    region,
        |    collect_set(guid) as guidList
        |from input
        |group by province, city, region
        |
        |""".stripMargin)

    result1.show()
    result1.createTempView("result1")

    spark.sql(
      """
        |select
        |    province,
        |    city,
        |    region,
        |    combine_unique(guidList) as newGuidList
        |from result1
        |group by province
        |
        |""".stripMargin).show()

    spark.close()

  }
}
