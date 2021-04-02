package bigdata.hermesfuxi.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo01 {
  def main(args: Array[String]): Unit = {
    //创建SparkSession(是对SparkContext的包装和增强)
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataFrameTxt = sparkSession.read.text("data/date.txt")
    //    dataFrameTxt.show()
    //    dataFrameTxt.printSchema()

    val dataFrame2 = sparkSession.read.csv("data/data.csv")
    //    dataFrame2.show()
    //    dataFrame2.printSchema()

    //schema: 字段名称、类型（csv文件中自带schema属性?）
    val dataFrame3: DataFrame = sparkSession.read
      .option("header", "true") //将第一行当做表头
      .option("inferSchema", "true") //推断数据类型
      .csv("data/emp.csv")
    //    dataFrame3.show()
    //    dataFrame3.printSchema()

    val schema = StructType(Array(
      StructField("name", StringType),
      StructField("salary", IntegerType),
      StructField("dept", StringType),
    ))
    val frame = sparkSession.createDataFrame(dataFrame3.rdd, schema)
    //    frame.write.csv("data/out")

    val jsonDataFrame = sparkSession.read.json("data/user.json")
    //    jsonDataFrame.createTempView("v_user")
    //    val resultJsonDF = sparkSession.sql("select * from v_user where name is not null")

    import sparkSession.implicits._

    val result = jsonDataFrame.select("name", "fv", "gender").where('name.isNotNull)
    result.show(2, 1)
    sparkSession.stop()
  }

}
