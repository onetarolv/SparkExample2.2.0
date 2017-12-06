package mllib.rddapi.classfication

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
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

    /** ---------spark examples-------------*/
//    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
//    val splits = data.randomSplit(Array(0.7, 0.3))
//    val (trainingData, testData) = (splits(0), splits(1))
//
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
//
//    val model = DecisionTree.trainClassifier(trainingData, numClasses,
//      categoricalFeaturesInfo, impurity, maxDepth, maxBins)
//
//    val labelAndPreds = testData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    labelAndPreds.take(10).foreach(println)
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
//    println("Test Error = " + testErr)
//    println("learned classification tree model:\n" + model.toDebugString)

//    val modelPath = "target/tmp/myDecisionTreeClassificationModel"
//    model.save(sc, modelPath)
//    val sameModel = DecisionTreeModel.load(sc, modelPath)

    /** ----------spark example end -------------*/

    /**-----------example for datasets "iris"-----------*/
    val category = Map("Iris-setosa" -> 0, "Iris-versicolor" -> 1, "Iris-virginica" -> 2)
    val iris = sc.textFile("data/mllib/classification/iris.txt").filter(_.nonEmpty)map(point => {
      val split = point.trim.split(",")
      new LabeledPoint(category(split(4)), Vectors.dense(split(0).toDouble, split(1).toDouble, split(2).toDouble, split(3).toDouble))
    })

    val irissplits = iris.randomSplit(Array(0.8, 0.2))
    val traindata = irissplits(0)
    val testdata = irissplits(1)

    val irismodel = DecisionTree.trainClassifier(traindata, 3, categoricalFeaturesInfo, impurity, 5, 4)
    println("real value | prediction | right?")
    val testResult = testdata.map(point => {
      val res = (point.label, irismodel.predict(point.features))
      (res._1, res._2, res._1 == res._2)
    })

    testResult.foreach(println)
    val testError = testResult.filter(!_._3).count().toDouble / testResult.count()
    println("testError = " + testError)
    println("learned classification tree model:\n" + irismodel.toDebugString)


    sc.stop()
  }
}
