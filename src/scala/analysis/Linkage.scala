package analysis


import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.{StatsWithMissing, NAStatCounter}
import org.apache.spark.{SparkContext, SparkConf}

import scala.Double


/**
 * Created by DELL_PC on 2018/3/1.
 */
object Linkage {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("Linkage")
    val sc = new SparkContext(conf)
    val rawblocks = sc.textFile("data/analysis/linkage")
    def isHeader(line: String): Boolean = {
      line.contains("id_1")
    }
    val noheader = rawblocks.filter(!isHeader(_))
    case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
    def parse(line: String): MatchData = {
      def toDouble(s: String) = {
        if(s.contains("?")) Double.NaN else s.toDouble
      }
      val pieces = line.split(",")
      val (id1, id2) = (pieces(0).toInt, pieces(1).toInt)
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      new MatchData(id1, id2, scores, matched)
    }
    val parsed = noheader.map(parse)

    /*
    val statsm = StatsWithMissing.statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = StatsWithMissing.statsWithMissing(parsed.filter(!_.matched).map(_.scores))
    statsm.zip(statsn).map{ case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)
    */

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    case class Scored(md: MatchData, score: Double)
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      new Scored(md, score)
    })

    val preds = ct.filter(s => s.score >= 3.0).map(s => s.md.matched).countByValue()
    println(preds)

    val matchCounts = parsed.map(md => md.matched).countByValue()

    val tf = preds(true).toDouble / matchCounts(true)

    val ff = 1.0 - preds(false).toDouble / matchCounts(false)
    println("real true : " + tf * 10 + "% real false : " + ff *10 + "%")
//    val matchCountSeq = matchCounts.toSeq
//
//    matchCountSeq.sortBy(_._2).reverse.foreach(println)
//    val nasRDD = parsed.map(_.scores.map(x => NAStatCounter(x)))
//    val reduced = nasRDD.reduce( (a, b) => {
////        (0 until 9).toArray.map(i => a(i).merge(b(i)))
//        a.zip(b).map { case (a, b) => a.merge(b) }
//      })

//    reduced.foreach(println)

//
//    import java.lang.Double.isNaN
//    val stat0 = parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()
//    println(stat0)
//
//    val stats = (0 until 9).map(i => {
//      parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
//    })
//    stats.foreach(println)

//    val nas1 = Array(1.0, 2.0, Double.NaN).map(x => NAStatCounter(x))
//    val nas2 = Array(Double.NaN, 6.0, Double.NaN).map(x => NAStatCounter(x))
//    val res = nas1.zip(nas2).map{case (r1, r2) => r1.merge(r2)}
//    res.map(x => x.toString).foreach(println)
//
//    val  arr = Array(1.2, 3.1, Double.NaN)
//    val  arrstat = arr.map(x => NAStatCounter(x)).reduce((a, b) => a.merge(b))
//    println(arrstat)
    sc.stop()
  }
}
