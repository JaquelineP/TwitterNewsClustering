package de.hpi.isg.mmds.sparkstreaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

object ClusterInfoAggregation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    val sc = new SparkContext(conf)

    aggregateClusterInfo(sc)
    aggregateTweets(sc)
  }

  def aggregateClusterInfo(sc: SparkContext) = {
    val directory = "output/merged_clusterInfo"
    FileUtils.deleteDirectory(new File(directory))

    val clusterInfo: RDD[(Int, Cluster, Long)] = sc.objectFile("output/batch_clusterInfo/batch-*")
    clusterInfo
        .sortBy( { case (clusterId, cluster, timestamp) => (timestamp, clusterId) }, ascending = true)
        .map{ case (clusterId, cluster, time) =>
          (clusterId, cluster.score.count, cluster.score.silhouette, cluster.score.intra, cluster.score.inter,
            cluster.representative.id, cluster.best_url, cluster.interesting, time, cluster.representative.content.text)
        }
      .saveAsTextFile(directory)
  }

  def aggregateTweets(sc: SparkContext) = {
    val directory = "output/merged_tweets"
    FileUtils.deleteDirectory(new File(directory))

    val clusterInfo: RDD[(Long, Int, Long, String)] = sc.objectFile("output/batch_tweets/batch-*")
    clusterInfo
      .sortBy(c => c._1 + c._2, ascending = true)
      .saveAsTextFile(directory)
  }

  def writeClusterInfo(outputStream: DStream[(Int, Cluster)]) = {
    outputStream
      .map{ case (clusterId, content) =>
        val time : Long = System.currentTimeMillis / 1000
        (content.fixed_id, content, time)
      }
      .saveAsObjectFiles("output/batch_clusterInfo/batch")
  }

  def writeTweets(outputStream: DStream[(Long, ((Int, TweetObj), Vector))], model: ExtendedStreamingKMeans) = {
    outputStream
      .map{ case (tweetId, ((clusterId, tweet), vector)) =>
        val time : Long = System.currentTimeMillis / 1000
        (time, model.fixedId(clusterId), tweetId, tweet.text)
      }
      .saveAsObjectFiles("output/batch_tweets/batch")
  }
}
