package de.hpi.isg.mmds.sparkstreaming

import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.DStream

class ExtendedStreamingKMeans extends StreamingKMeans {

  private def updateModel(newCenters: Array[Vector], newWeights: Array[Double]): this.type = {
    model = new StreamingKMeansModel(newCenters, newWeights)
    setK(newCenters.length)
    this
  }

  def addCentroids(centers: Array[Vector], weights: Array[Double]): this.type = {
    val newCenters = model.clusterCenters ++ centers
    val newWeights = model.clusterWeights ++ weights
    this.updateModel(newCenters, newWeights)
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
    updateModel(newCenters, newWeights)
  }

  override def trainOn(data: DStream[Vector]) {
    data.foreachRDD { (rdd) =>
      val distantTweets = rdd.filter { (vector) =>
        val center = model.predict(vector)
        Vectors.sqdist(vector, latestModel().clusterCenters(center)) > 100
      }.collect()

      var newCenters = Array[Vector]()
      distantTweets.foreach { tweet =>
        var addNewCenter = true
        newCenters.foreach(center => addNewCenter &&= Vectors.sqdist(tweet, center) > 50)
        if (addNewCenter) newCenters +:= tweet
      }
      addCentroids(newCenters, Array.fill[Double](newCenters.length)(1.0))
    }
    super.trainOn(data)
  }

}
