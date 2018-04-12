package syntax.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by engry on 2018/4/12.
 */
object TextSearch {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("TextSearch").master("local").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("F:\\Spark\\test.txt")
    import spark.implicits._
    val df = rdd.toDF("line")
    val errors = df.filter($"line".like("%TASKScheduler%"))
    df.show();
    spark.close();
  }
}
