package de.hpi.isg.mmds.sparkstreaming

import java.io.File

import breeze.linalg.max
import de.hpi.isg.mmds.sparkstreaming.nlp.NLPPipeline
import de.hpi.isg.mmds.sparkstreaming.twitter.TweetStream
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import HashAggregation.writeHashes
import ClusterInfoAggregation.writeClusterInfo
import ClusterInfoAggregation.writeTweets
import org.apache.spark.mllib.clustering.KMeans

case class TwitterClustering(args: Main.MainArgs.type) {

  var conf: SparkConf = null
  var ssc: StreamingContext = null
  var model: ExtendedStreamingKMeans = null

  def createStreamingContext() = {
    conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    ssc = new StreamingContext(conf, Seconds(args.batchDuration))
  }

  def createKMeansModel() = {
    model = new ExtendedStreamingKMeans()
      .setK(1)
      .setDecayFactor(args.forgetfulness)
      .setInitialCenters(Array.fill(1)(Vectors.dense(Array.fill(args.vectorDimensions)(100.0))), Array.fill(1)(0.0))
      .setAddThreshold(args.addThreshold)
      .setMergeThreshold(args.mergeThreshold)
  }

  def preprocessTweets(tweetIdTextStream: DStream[Tweet]) = {

    val tweetIdVectorsCollisionMapStream = tweetIdTextStream.transform(tweetRdd => {

      if (!tweetRdd.isEmpty())
        NLPPipeline(args.vectorDimensions).preprocess(tweetRdd.map { case tweet => (tweet.id, tweet.content.text) })
      else
        tweetRdd.sparkContext.emptyRDD[(Long, Vector, Map[Int, Seq[String]])]
    })

    writeHashes(tweetIdVectorsCollisionMapStream)

    tweetIdVectorsCollisionMapStream.map {
      case (tweetId, vector, hashWordList) => (tweetId, vector)
    }
  }

  def clusterTweets(tweetIdVectorsStream: DStream[(Long, Vector)]) = {
    val model = this.model
    val vectorsStream = tweetIdVectorsStream.map{ case (tweetId, vector) => vector }
    model.trainOn(vectorsStream)
    tweetIdVectorsStream.map { case (id, vector) => (id, model.latestModel.predict(vector)) }
  }

  def createClusterInfoStream(joinedStream: DStream[(Long, ((Int, TweetObj), Vector))], neighborStream: DStream[(Int, Int)]) = {
    val model = this.model

    joinedStream

      // add squared distances
      .map {
      case (tweetId, ((clusterId, tweetObj), vector)) =>
        val center = model.latestModel().clusterCenters(clusterId)
        val sum = KMeans.vectorSum(vector)
        (clusterId, (1, vector, sum, VectorUtils.scaledDist(vector, center, sum), new Tweet(tweetId, tweetObj), Array[String]()))
    }

      // add neighbor
      .join(neighborStream)
      .map {
        case (clusterId, ((count, vector, sum, sqDist, tweet, urls), neighbor)) =>
          val center = model.latestModel().clusterCenters(neighbor)
          (clusterId, (count, sqDist, tweet, urls, VectorUtils.scaledDist(vector, center, sum)))
      }

      .reduceByKey {
        case ((countA, sqDistA, tweetA, urlsA, neighborDistA), (countB, sqDistB, tweetB, urlsB, neighborDistB)) => (
          countA + countB,
          sqDistA + sqDistB,
          if (sqDistA >= sqDistB) tweetB else tweetA,
          tweetA.content.urls ++ tweetA.content.urls,
          neighborDistA + neighborDistB
          )
      }

      .map {
        case (clusterId, (count, distanceSum, tweet, urls, neighborDistanceSum)) =>
          // determine most frequently occurring url
          val urlGroups = urls.groupBy(identity).mapValues(_.length)
          val best_url = if (urlGroups.isEmpty) "none" else urlGroups.maxBy{case (url, occurrences) => occurrences}._1

          // calculate average distance to center from sum of distances and count
          val avgSqDist = distanceSum / count
          val avgNeighborDist = neighborDistanceSum / count

          // calculate "kind-of" silhouette for every cluster
          val silhouette = (avgNeighborDist - avgSqDist) / max(avgNeighborDist, avgSqDist)

          // mark clusters with more than 8 tweets and silhouette >= 0.4 & <= 0.95 as interesting
          val interesting = (count >= 8) && (silhouette >= 0.4) && (silhouette <= 0.95)

          def round(x :Double) = {
            try {
              BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            } catch {
              case e: Exception =>
                0.0
            }
          }

          val cluster = new Cluster(new Score(count, round(silhouette), round(avgSqDist), round(avgNeighborDist)),
            interesting, tweet, best_url, model.fixedId(clusterId))
          (clusterId, cluster)
      }

      // sort by silhouette
      .transform(rdd => rdd.sortBy( { case (clusterId, cluster) => cluster.score.silhouette }, ascending = false, 1))

  }


  def outputClusterInfos(clusterInfoStream: DStream[(Int, Cluster)]) = {
    var lastTime = System.nanoTime
    clusterInfoStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val batchSize = rdd
          .map { case (clusterId, cluster) => cluster.score.count }
          .reduce(_+_)

        val totalSilhouette = rdd
          .map { case (clusterId, cluster) => cluster.score.count * cluster.score.silhouette }
          .reduce(_+_)

        val interesting = rdd
          .filter{ case (clusterId, cluster) => cluster.interesting}
          .count()


        val elapsed = (System.nanoTime - lastTime).toDouble / 1000000000
        lastTime = System.nanoTime
        if (args.runtimeMeasurements) {
          print(s"$elapsed,")
        } else {
          println("\n-------------------------\n")
          println(s"New batch: $batchSize tweets")
          println(f"Processing time: $elapsed%.2fs")
          println(s"Cluster count: ${model.k}")
          println(s"interesting: $interesting")
          println(f"Average silhouette: ${totalSilhouette / batchSize}%.2f")

          /*rdd.foreach {
            case (clusterId, cluster) =>
              val score = cluster.score
              val tweet = cluster.representative
              val url = cluster.best_url
              println(s"clusterId: ${cluster.fixed_id} count: ${score.count}, silhouette: ${score.silhouette}, " +
                s"intra-distance: ${score.intra}, inter-distance: ${score.inter}, " +
                s"representative: ${tweet.id}, interesting: ${cluster.interesting}, url: $url")
          }*/
        }
      } else if (args.runtimeMeasurements) System.exit(0)
    })
  }

  def execute() {

    FileUtils.deleteDirectory(new File("output/batch_clusterInfo"))
    FileUtils.deleteDirectory(new File("output/batch_collisions"))
    FileUtils.deleteDirectory(new File("output/batch_tweets"))
    createStreamingContext()

    // set log level
    LogManager.getRootLogger.setLevel(Level.OFF)

    // create Twitter Stream (tweet id, tweet text)
    val tweetIdTextStream: DStream[Tweet]  = TweetStream.create(this)
    createKMeansModel()

    // preprocess tweets and create vectors
    val tweetIdVectorsStream: DStream[(Long, Vector)] = this.preprocessTweets(tweetIdTextStream)

    // run Clustering Algorithm and retrieve clusters (tweet Id, cluster Id)
    val tweetIdClusterIdStream: DStream[(Long, Int)] = this.clusterTweets(tweetIdVectorsStream)

    // contains (tweetId, ((clusterId, tweet), vector))
    val joinedStream = tweetIdClusterIdStream
      .join(tweetIdTextStream.map(tweet => (tweet.id, tweet.content)))
      .join(tweetIdVectorsStream)
    writeTweets(joinedStream, this.model)

    val model = this.model
    val clusterNeighborStream = tweetIdClusterIdStream
      .map { case (tweetId, clusterId) => (clusterId, tweetId) }
      .reduceByKey { case (id1, id2) => id1 }
      .map { case (clusterId, tweetId) =>
        val center = model.latestModel.clusterCenters(clusterId)
        val neighbor = model.latestModel.clusterCenters.zipWithIndex.minBy {
          case (otherCenter, index) =>
            if (otherCenter != center) Vectors.sqdist(center, otherCenter) else Double.MaxValue
        }._2
        (clusterId, neighbor)
      }

    // contains (clusterId, clusterContent)
    val clusterInfoStream = this.createClusterInfoStream(joinedStream, clusterNeighborStream)
    writeClusterInfo(clusterInfoStream)

    this.outputClusterInfos(clusterInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
