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
    var newCenters = model.clusterCenters
      .zipWithIndex
      .filter { case (vector, index) => !centers.contains(index) }
      .map { case (vector, index) => vector }
    var newWeights = model.clusterWeights
      .zipWithIndex
      .filter { case (weight, index) => !centers.contains(index) }
      .map { case (weight, index) => weight }

    if (newCenters.length == 0) {
      newCenters = Array.fill(1)(Vectors.dense(Array.fill(model.clusterCenters(0).size)(-1.0)))
      newWeights = Array.fill(1)(0.0)
    }

    updateModel(newCenters, newWeights)
  }

  private def addClusters(data: DStream[Vector]): Unit = {
    data.foreachRDD { rdd =>
      val distantVectors = rdd.filter { (vector) =>
        val center = model.predict(vector)
        Vectors.sqdist(vector, model.clusterCenters(center)) > 100
      }.collect()

      var newCenters = Array[Vector]()
      distantVectors.foreach { vector =>
        var addNewCenter = true
        newCenters.foreach(center => addNewCenter &&= Vectors.sqdist(vector, center) > 50)
        if (addNewCenter) newCenters +:= vector
      }
      addCentroids(newCenters, Array.fill[Double](newCenters.length)(0.0))
    }
  }

  private def mergeClusters(data: DStream[Vector]): Unit = {
    data.foreachRDD { rdd =>
      val centerRDD = rdd.sparkContext.parallelize(model.clusterCenters.zipWithIndex)
      val clustersToMerge = centerRDD
        .cartesian(centerRDD)
        .filter { case ((vec1, index1), (vec2, index2)) => (index1 < index2) && (Vectors.sqdist(vec1, vec2) < 50) }
        .map { case ((vec1, index1), (vec2, index2)) => (index1, index2) }
        .collect()

      var clustersToDelete = Array[Int]()
      clustersToMerge.foreach { case (index1, index2) =>
        if (!clustersToDelete.contains(index1) && !clustersToDelete.contains(index2)) {
          clustersToDelete +:= index2
        }
      }
      removeCentroids(clustersToDelete)
    }
  }

  private def deleteOldClusters(data: DStream[Vector]): Unit = {
    data.foreachRDD { rdd =>
      val clustersToDelete = rdd.sparkContext.parallelize(model.clusterWeights)
        .zipWithIndex
        .filter { case (weight, index) => weight < 1.0 }
        .map { case (weight, index) => index.toInt }
        .collect()
      removeCentroids(clustersToDelete)
    }
  }

  override def trainOn(data: DStream[Vector]) {
    addClusters(data)
    super.trainOn(data)
    mergeClusters(data)
    deleteOldClusters(data)
  }

}
