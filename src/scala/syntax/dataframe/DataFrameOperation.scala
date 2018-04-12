package syntax.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataFrameOperation {
  def getDFbyReadJson(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.json(filePath)
  }

  def sqlDF(spark: SparkSession, sqlcode: String): DataFrame = {
    spark.sql(sqlcode)
  }

  def createOrReplaceTempView(spark: SparkSession, df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
   // spark.sql("select name from people").show()

//    sqlDF(spark, "select name from people").show()
  }
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local").appName("DataFrameOP")
      .getOrCreate()

    import spark.implicits._
    val df = getDFbyReadJson(spark, "data/syntax/sql/people.json")
    df.select($"name", $"age" + 1).show()
    val tableName = "people"
//    df.createOrReplaceTempView(tableName)
//    spark.sql("select name from people").show()
    createOrReplaceTempView(spark,df, tableName)

    sqlDF(spark, "select name from people").show()

    spark.stop()
  }
}
