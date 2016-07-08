package de.hpi.isg.mmds.sparkstreaming

import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansModel}
import org.apache.spark.mllib.linalg.Vector

class ExtendedStreamingKMeans extends StreamingKMeans {

  def addCentroids(centers: Array[Vector], weights: Array[Double]): this.type = {
    val newCenters = model.clusterCenters ++ centers
    val newWeights = model.clusterWeights ++ weights
    model = new StreamingKMeansModel(newCenters, newWeights)
    setK(newCenters.length)
    this
  }

  def removeCentroids(centers: Array[Int]): this.type = {
    val newCenters = model.clusterCenters
      .zipWithIndex
      .filter { case (vector, index) => !centers.contains(index) }
      .map { case (vector, index) => vector }
    val newWeights = model.clusterWeights
      .zipWithIndex
      .filter { case (weight, index) => !centers.contains(index) }
      .map { case (weight, index) => weight }
    model = new StreamingKMeansModel(newCenters, newWeights)
    setK(newCenters.length)
    this
  }

}
