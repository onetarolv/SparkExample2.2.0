package mllib.APIGuide

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by engry on 2017/11/24.
 */
object KMeansExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
    val spark = SparkSession.builder()
      .appName("KMeansExample")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    val numClusters = 2
    val numIterations = 10
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val forecast = clusters.predict(Vectors.dense(8,8,3))
    println("The prediction of Vector(8,8,3) = "+ forecast)
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    val savePath = "target/org/apache/spark/KMeansExample/KMeansModel"
    clusters.save(sc, savePath)
//
//    val sameModel = KMeansModel.load(sc, savePath)

    val model = new KMeans().setInitializationMode(KMeans.K_MEANS_PARALLEL)
      .setEpsilon(0.01).setMaxIterations(20)
    val clusters2 = model.run(parsedData)
    val WSSSE2 =  clusters2.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE2)

    spark.stop()
  }

}
