package mllib.APIGuide

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{GaussianMixtureModel, GaussianMixture}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by engry on 2017/11/24.
 */
object GaussionMixtureExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
    val spark = SparkSession.builder()
      .appName("GMExample")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile("data/mllib/gmm_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

    val gmm = new GaussianMixture().setK(2).run(parsedData)

    val savePath = "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel"
    gmm.save(sc, savePath)

    val sameModel = GaussianMixtureModel.load(sc, savePath)
    for(i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }

    spark.stop()
  }
}
