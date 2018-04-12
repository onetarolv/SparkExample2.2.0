package codepractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by engry on 2018/4/12.
 */
object AverageCalculation {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("AverageCalculation")
    val sc = new SparkContext(conf)

    val txt = sc.textFile("data/syntax/calculate_avg").filter(x => !x.startsWith("ID"))
      .map(x => (x.split(",")(2), x.split(",")(3).toInt))
    txt.foreach(println)
    println("========")
//    val groupedavg = txt.combineByKey(
//      (v) => (v, 1),
//      (accu: (Int, Int), v) => (accu._1 + v, accu._2 + 1),
//      (accu1: (Int, Int), accu2: (Int, Int)) => (accu1._1 + accu2._1, accu1._2 + accu2._2)
//    ).mapValues(x => (x._1 / x._2).toDouble).collect().foreach(println)
    val groupedavg = txt.map(x => (x._1, (x._2, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(x => (x._1, x._2._1 / x._2._2)).collect()
    groupedavg.foreach(println)
    val totalcount = txt.map(x => (x._2, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val totalavg = totalcount._1 / totalcount._2
    println("total avg = " + totalavg)
    sc.stop()
  }
}
