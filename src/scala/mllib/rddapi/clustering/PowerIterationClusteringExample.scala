package mllib.rddapi.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.PowerIterationClustering

/**
 * Created by engry on 2017/11/28.
 */
object PowerIterationClusteringExample {
  case class Params(
                   k: Int = 2,
                   numPoints: Int = 10,
                   maxIterations: Int = 15
                     ) extends AbstractParams[Params]
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\envpath\\hadoop-2.6.4");
    val spark = SparkSession.builder()
      .appName("PICExample")
      .master("local")
      .getOrCreate()

    val k = 10
    val numPoints = 10
    val sc = spark.sparkContext
    val circlesRdd = generateCirclesRdd(sc, k, numPoints)
    val model = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations = 80)
      .setInitializationMode("degree")
      .run(circlesRdd)

    val clusters = model.assignments.collect()
      .groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }mkString(",")

    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
    spark.stop()
  }

  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))}
  }

  def generateCirclesRdd(
                        sc: SparkContext,
                        nCircles: Int,
                        nPoints: Int): RDD[(Long, Long, Double)] = {
    /*
    * nCircles means the number of circles
    * i denotes the radius of each circle
    * i * nPoints denotes the number of points on each circle
    * */
    val points = (1 to nCircles).flatMap( i =>
      generateCircle(i, i * nPoints))
      .zipWithIndex

    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1,y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p2._1 - p2._2)
    math.exp(-ssquares / 2)
  }
}
