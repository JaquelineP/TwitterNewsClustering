package de.hpi.isg.mmds.sparkstreaming

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

    val clusterInfo: RDD[(Int, Int, Double, Double, Double, Long, String, Boolean)] = sc.objectFile("output/batch_clusterInfo/batch-*")

    clusterInfo.saveAsTextFile("output/merged_clusterInfo")

  }

  def writeClusterInfo(outputStream: DStream[(Int, (Int, (Double, Double, Double), Long, String, Boolean))]) = {
    outputStream
      .map{ case (clusterId, (count, (silhouette, intra, inter), representative, url, interesting)) =>
        (clusterId, count, silhouette, intra, inter, representative, url, interesting)
      }

      .saveAsObjectFiles("output/batch_clusterInfo/batch")
  }

}
