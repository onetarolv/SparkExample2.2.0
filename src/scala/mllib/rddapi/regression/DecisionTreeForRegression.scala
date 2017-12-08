package mllib.rddapi.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by engry on 2017/12/8.
 */
object DecisionTreeForRegression {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DTForRegression")
    val sc = new SparkContext(conf)

    /** spark example */
//    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
//    val splits = data.randomSplit(Array(0.7,0.3))
//    val (trainningData, testData) = (splits(0), splits(1))
//
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val impurity = "variance"
//    val maxDepth = 5
//    val maxBins = 32
//
//    val model = DecisionTree.trainRegressor(trainningData, categoricalFeaturesInfo,
//    impurity, maxDepth, maxBins)
//
//    val labelsAndPredictions = testData.map{ point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    println("real value | prediction")
//    labelsAndPredictions.take(10)
//      .foreach(res => println(res._1 + " | " + res._2))
//    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v-p, 2) }.mean()
//    println("Test Mean Squared Error = " + testMSE)
//    println("Learned regression tree model:\n" + model.toDebugString)

//    val modelPath = "target/tmp/myDecisionTreeClassificationModel"
//    model.save(sc, modelPath)
//    val sameModel = DecisionTreeModel.load(sc, modelPath)

    /**--------- end ----------*/

    val data = sc.textFile("data/mllib/regression/airfoil_self_noise.txt").filter(_.nonEmpty)
      .map { dataStr =>
        val dataSplit = dataStr.trim.split("\\s+").map(_.toDouble)
        val point = new Array[Double](dataSplit.length-1)
        dataSplit.copyToArray(point, 0, point.length)
        (new LabeledPoint(dataSplit.last, Vectors.dense(point)))
      }
    val splits = data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))

    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 6
    val maxBins = 32

    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo,
    impurity, maxDepth, maxBins)

    val labelsAndPredictions = testData.map{ point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    println("real value | prediction")
    labelsAndPredictions.take(10)
      .foreach(res => println(res._1 + " | " + res._2))
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v-p, 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression tree model:\n" + model.toDebugString)

    sc.stop()

  }
}
