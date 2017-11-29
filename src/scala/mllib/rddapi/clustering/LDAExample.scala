package mllib.rddapi.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by engry on 2017/11/29.
 */
object LDAExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val hadoopPath = args(0)
    System.setProperty("hadoop.home.dir", hadoopPath);

    val conf = new SparkConf().setAppName("LDAExample").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/mllib/sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    val cropus = parsedData.zipWithIndex().map(_.swap).cache()

    val k = 3

    val ldaModel = new LDA().setK(k).run(cropus)

    println("Learned topics (as distributions over vocab of "
      + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for(topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for(word <- Range(0,ldaModel.vocabSize)) {
        print(" " + topics(word, topic));
      }
      println()
    }

    val savePath = "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel"
    ldaModel.save(sc, savePath)
    val sameModel = DistributedLDAModel.load(sc, savePath)

    sc.stop()
  }
}
