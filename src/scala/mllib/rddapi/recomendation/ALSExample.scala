package mllib.rddapi.recomendation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by DELL_PC on 2018/2/27.
 */
object ALSExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("ALSExample")
    val sc = new SparkContext(conf)

    /*
    val data = sc.textFile("data/mllib/als/test.data")
    val ratings = data.map(_.split(",") match { case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, lambda = 0.01)

    val usersProducts = ratings.map{ case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map{ case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.take(10).foreach(println)

    val MSE = ratesAndPreds.map{ case ((user, product), (r1, r2)) =>
      val err = (r1- r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    */
    //model.save(sc, "target/tmp/myALS")

    /*
    val movielens = sc.textFile("data/mllib/als/sample_movielens_ratings.txt")
    val movieRatings = movielens.map(_.split("::") match { case Array(user, movie, rate, useless) =>
        Rating(user.toInt, movie.toInt, rate.toDouble)
    })

    val splits = movieRatings.randomSplit(Array(0.8, 0.2))
    val (training, test) = (splits(0), splits(1))

    val movielensModel = ALS.train(training, rank = 15, iterations = 10, lambda = 0.01)

    val movieRealValues = test.map { case Rating(user, movie, rate) => ((user, movie), rate)}
    val testset = movieRealValues.map { case ((user, movie), rate) => (user, movie)}

    val moviePredictions = movielensModel.predict(testset).map { case Rating(user, movie, rate) =>
      ((user, movie), rate)
    }.join(movieRealValues)

    moviePredictions.take(10).foreach(println)
    val movieMSE = moviePredictions.map { case ((user, movie), (r1, r2)) => (r1 - r2) * (r1 - r2)}.mean()
    println("Mean Squared Error = " + movieMSE)
    */
    //movielensModel.save(sc, "target/tmp/recomendation/movielens_ALS")
    //val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/recomendation/movielens_ALS")

    val movielensData = sc.textFile("data/mllib/als/sample_movielens_ratings.txt")
    val movieRatings = movielensData.map(_.split("::") match { case Array(user, item, rate, useless) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 10
    val numIterations = 10
    val movieModel = ALS.train(movieRatings, rank, numIterations, lambda = 0.01)

    val usersProducts = movieRatings.map{ case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = movieModel.predict(usersProducts).map{ case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    val ratesAndPreds = movieRatings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.take(10).foreach(println)

    val MSE = ratesAndPreds.map{ case ((user, product), (r1, r2)) =>
      val err = (r1- r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    movieModel.save(sc, "target/tmp/recomendation/movielens_ALS")
    val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/recomendation/movielens_ALS")
    sc.stop()
  }

}
