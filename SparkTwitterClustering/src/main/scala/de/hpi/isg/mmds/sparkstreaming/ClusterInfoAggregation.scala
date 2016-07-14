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
    // round to 2 decimals
    def round(x: Double) = x - (x % 0.01)

    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    val sc = new SparkContext(conf)

    val clusterInfo: RDD[(Int, Cluster, Long)] = sc.objectFile("output/batch_clusterInfo/batch-*")
    clusterInfo
        .sortBy(c => c._3 + c._1, true)
        .map{ case (clusterId, cluster, time) =>
          (clusterId, cluster.score.count, round(cluster.score.silhouette), round(cluster.score.intra), round(cluster.score.inter),
            cluster.representative.id, cluster.best_url, cluster.interesting, time, cluster.representative.content.text)
        }
      .saveAsTextFile("output/merged_clusterInfo")
  }

  def writeClusterInfo(outputStream: DStream[(Int, Cluster)]) = {
    outputStream
      .map{ case (clusterId, content) =>
        val time : Long = System.currentTimeMillis / 1000
        (content.fixed_id, content, time)
      }
      .saveAsObjectFiles("output/batch_clusterInfo/batch")
  }

}
