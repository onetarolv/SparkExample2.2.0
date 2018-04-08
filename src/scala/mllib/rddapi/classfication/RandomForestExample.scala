package mllib.rddapi.classfication

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by DELL_PC on 2018/3/1.
 */
object RandomForestExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("ALSExample")
    val sc = new SparkContext(conf)
    /*
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesinfo = Map[Int, Int]()
    val numTrees = 3
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

//    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesinfo,
//    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesinfo,
    numTrees, featureSubsetStrategy, "variance", maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)
    */

    val data = sc.textFile("data/mllib/classification/iris.txt").filter(_.nonEmpty)
    val category = Map("Iris-setosa" -> 0, "Iris-versicolor" -> 1, "Iris-virginica" -> 2)

    val irisData = data.map(_.split(",")).map {case Array(f1, f2, f3, f4, label) =>
      new LabeledPoint(category(label), Vectors.dense(f1.toDouble, f2.toDouble, f3.toDouble, f4.toDouble))
    }

    val splits = irisData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val model = RandomForest.trainClassifier(trainingData, numClasses = 4, categoricalFeaturesInfo = Map.empty,
      numTrees = 5, featureSubsetStrategy = "auto", impurity = "gini", maxDepth = 5, maxBins = 32)

    val labelAndPreds = testData.map { point =>
      val pred = model.predict(point.features)
      (point.label, pred)
    }
    labelAndPreds.foreach(println)
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)
    sc.stop()
  }
}
