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

  def writeClusterInfo(outputStream: DStream[(Int, (Int, (Double, Double, Double), Long, String, Boolean))], representatives: DStream[(Long, ((Int, (String, Array[String])), Vector))]) = {
    val text : DStream[(Long, String)] = representatives.map{ case (tweetId, ((clusterId, (text, urls)), vector)) =>
      (tweetId, text)
    };

    outputStream
      .map{ case (clusterId, (count, (silhouette, intra, inter), representative, url, interesting)) =>
        val time : Long = System.currentTimeMillis / 1000
        (representative, (clusterId, count, silhouette, intra, inter, url, interesting, time))
      }
      .join(text)
      .map{ case (representative, ((clusterId, count, silhouette, intra, inter, url, interesting, time), text)) =>
        (clusterId, count, silhouette, intra, inter, representative, url, interesting, time, text)
      }
      .saveAsObjectFiles("output/batch_clusterInfo/batch")
  }

}
