package analysis

import java.io.File
import java.nio.file.{FileSystems, FileSystem}

import filesystem.FileSystemOperation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by DELL_PC on 2018/4/26.
 */
object Transfer {
  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local").setAppName("transfer")
//    val sc = new SparkContext(conf)
//    val file = sc.textFile("C:\\Users\\DELL_PC\\Desktop\\test.csv").map(line => {
//      val values = line.split(",")
//      val value = values(0).trim.split("\\s+")(0)
//      line + "," + value
//    })
//    file.saveAsTextFile("data/test")

//    sc.stop()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("transfer").master("local").getOrCreate();
    import spark.implicits._


    val df = spark.read.option("header", "true").csv("C:\\Users\\DELL_PC\\Desktop\\test.csv")
    df.printSchema()
    val df2 = df.withColumn("date", $"time".substr(0, 10))
    val path = "data/test"

    val fsop = new FileSystemOperation()
    fsop.deleteFile(path)
    df2.write.csv("data/test")
    spark.stop()
  }
}
