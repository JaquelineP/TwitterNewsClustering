package de.hpi.isg.mmds.sparkstreaming

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object VectorUtils {

  def scaledDist(v1: Vector, v2: Vector, v1Sum: Double): Double = {
    Vectors.sqdist(v1, v2) / v1Sum
  }

  def scaledDist(v1: Vector, v2: Vector): Double = {
    scaledDist(v1, v2, KMeans.vectorSum(v1))
  }

}
