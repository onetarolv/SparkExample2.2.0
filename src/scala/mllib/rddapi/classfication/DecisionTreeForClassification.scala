package mllib.rddapi.classfication

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by engry on 2017/12/1.
 */
object DecisionTreeForClassification {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("DTForClassification")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses,
      categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("learned classification tree model:\n" + model.toDebugString)

    val modelPath = "target/tmp/myDecisionTreeClassificationModel"
    model.save(sc, modelPath)
    val sameModel = DecisionTreeModel.load(sc, modelPath)

    sc.stop()
  }
}
