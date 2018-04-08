package org.apache.spark.util

import java.lang.Double.isNaN

import org.apache.spark.rdd.RDD

/**
 * Created by DELL_PC on 2018/3/2.
 */
class NAStatCounter extends Serializable{
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if(isNaN(x))
      missing += 1
    else
      stats.merge(x)
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString = {
    "stats: " + stats.toString() + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}

object StatsWithMissing {
  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions( (iter: Iterator[Array[Double]]) => {
      val nas = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach{ case (n, d) => n.add(d)}
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map{ case (a, b) => a.merge(b)}
    })
  }
}