package syntax.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{Row, Dataset, DataFrame, SparkSession}

case class Person(name: String, age: Long)

class DataFrameOperation(spark: SparkSession) {

  import spark.implicits._

  def getDFbyReadJson(filePath: String): DataFrame = {
    spark.read.json(filePath)
  }

  def sqlDF(sqlcode: String): DataFrame = {
    spark.sql(sqlcode)
  }

  def createOrReplaceTempView(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
  }

  def createGlocalTempView(df: DataFrame, tableName: String): Unit = {
    df.createGlobalTempView(tableName)
  }


  def createDataSet[T]():Unit = {
    /** fist, crerate by toDS()
      *  Encoder are created for :1. case class;  2. common types */
    val caseClassDS = Seq(Person("Andy", 32), Person("Mile", 21)).toDS()
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).show()

    /** second, create by DataFrame.as[ClassName] */
    val path = "data/syntax/sql/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

  def transferRDD2DFByCaseClass(): DataFrame = {
    val df = spark.sparkContext
      .textFile("data/syntax/sql/people.txt")
      .map(attr => Person(attr.split(",")(0), attr.split(",")(1).trim.toLong))
      .toDF()
    df.show()
    df.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val map = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    map.foreach(println)
    df
  }
  def transferRDD2DFBySchema(): DataFrame = {
    val rowRDD = spark.sparkContext
      .textFile("data/syntax/sql/people.txt")
      .map(attr => Row(attr.split(",")(0), attr.split(",")(1).trim))

    val schemaStr = "name age"
    val fields = schemaStr.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val df = spark.createDataFrame(rowRDD, schema)
    df.show();
    df
  }

}

object DataFrameOperation {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local").appName("DataFrameOP")
      .getOrCreate()
    val dfop = new DataFrameOperation(spark)
    import spark.implicits._

//    val df = dfop.getDFbyReadJson("data/syntax/sql/people.json")
//    df.select($"name", $"age" + 1).show()
//    val tableName = "people"
    dfop.transferRDD2DFBySchema();
    //    createOrReplaceTempView(spark,df, tableName)
    //    createGlocalTempView(spark, df, tableName)
    //    sqlDF(spark, "select * from global_temp.people").show()

    spark.stop()
  }
}
