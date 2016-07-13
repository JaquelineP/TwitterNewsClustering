package de.hpi.isg.mmds.sparkstreaming

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}


object ClusterInfoAggregation {


  def main(args: Array[String]): Unit = {
    aggregate()
  }

  def aggregate() = {
    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    val sc = new SparkContext(conf)

    val clusterInfo: RDD[(Int, Int, Double, Double, Double, Long, String, Boolean, Long, String)] = sc.objectFile("output/batch_clusterInfo/batch-*")
    clusterInfo
        .sortBy(c => c._9 + c._1, true)
      .saveAsTextFile("output/merged_clusterInfo")
  }

  def writeClusterInfo(outputStream: DStream[(Int, (Int, (Double, Double, Double), Long, String, String, Boolean))]) = {
    outputStream
      .map{ case (clusterId, (count, (silhouette, intra, inter), representative, url, text, interesting)) =>
        val time : Long = System.currentTimeMillis / 1000
        (clusterId, count, silhouette, intra, inter, representative, url, interesting, time, text)
      }
      .saveAsObjectFiles("output/batch_clusterInfo/batch")
  }

}
