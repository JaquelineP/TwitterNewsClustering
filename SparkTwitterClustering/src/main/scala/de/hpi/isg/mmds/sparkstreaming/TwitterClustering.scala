package de.hpi.isg.mmds.sparkstreaming

import breeze.linalg.max
import de.hpi.isg.mmds.sparkstreaming.nlp.NLPPipeline
import de.hpi.isg.mmds.sparkstreaming.twitter.TweetStream
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import HashAggregation.writeHashes
import ClusterInfoAggregation.writeClusterInfo

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
      .setInitialCenters(Array.fill(1)(Vectors.dense(Array.fill(args.vectorDimensions)(-1.0))), Array.fill(1)(0.0))
  }

  def preprocessTweets(tweetIdTextStream: DStream[(Long, (String, Array[String]))]) = {

    val tweetIdVectorsCollisionMapStream = tweetIdTextStream.transform(tweetRdd => {

      if (!tweetRdd.isEmpty())
        NLPPipeline(args.vectorDimensions).preprocess(tweetRdd.map { case (id, (text, urls)) => (id, text) })
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

  def createClusterInfoStream(joinedStream: DStream[(Long, ((Int, (String, Array[String])), Vector))]) = {
    val model = this.model

    joinedStream

      // add squared distances
      .map {
        case (tweetId, ((clusterId, (text, urls)), vector)) =>
          val center = model.latestModel().clusterCenters(clusterId)
          (clusterId, (1, Vectors.sqdist(vector, center), tweetId, text, urls))
      }
        .reduceByKey {
        case ((countA, sqDistA, tweetIdA, textA, urlsA), (countB, sqDistB, tweetIdB, textB, urlsB)) => (
          countA + countB,
          sqDistA + sqDistB,
          if (sqDistA >= sqDistB) tweetIdB else tweetIdA,
          if (sqDistA >= sqDistB) textB else  textA,
          urlsA ++ urlsB
        )
      }

      // calculate average distance to center from sum of distances and count
      .map {
        case (clusterId, (count, distanceSum, representative, text, urls)) =>
          (clusterId, (count, distanceSum / count, representative, text, urls))
      }

      // determine most frequently occurring url
      .map {
        case (clusterId, (count, distanceAvg, representative, text, urls)) =>
          val urlGroups = urls.groupBy(identity).mapValues(_.length)
          val best_url = if (urlGroups.isEmpty) "none" else urlGroups.maxBy{case (url, occurrences) => occurrences}._1
          (clusterId, (count, distanceAvg, representative, text, best_url))
      }

      // calculate "kind-of" silhouette for every cluster
      .map {
        case (clusterId, (count, avgSqDist, representative, text, url)) =>
          val center = model.latestModel().clusterCenters(clusterId)
          // calculate distance to all other cluster centers, and choose lowest
          val neighborDistance = Vectors.sqdist(center,
            model.latestModel.clusterCenters.minBy(otherCenter =>
              if (otherCenter != center) Vectors.sqdist(center, otherCenter) else Double.MaxValue))
          val silhouette = (neighborDistance - avgSqDist) / max(neighborDistance, avgSqDist)
          (clusterId, (count, (silhouette, avgSqDist, neighborDistance), representative, text, url))
      }

      // sort by silhouette
      .transform(rdd => rdd.sortBy( { case (clusterId, (count, silhouette, representative, text, url)) => silhouette._1 }, ascending = false, 1))

      // mark clusters with more than 2 tweets and silhouette >= 0 as interesting
      .map{
        case (clusterId, (count, silhouette, representative, text, url)) =>
          (clusterId, (count, silhouette, representative, text, url, (count >= 3) && (silhouette._1 >= 0)))
      }
  }


  def outputClusterInfos(clusterInfoStream: DStream[(Int, (Int, (Double, Double, Double), Long, String, String, Boolean))]) = {
    var lastTime = System.nanoTime
    clusterInfoStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val batchSize = rdd.map {
          case (clusterId, (count, distanceSum, representative, url, text, interesting)) => count
        }.reduce(_+_)

        val elapsed = (System.nanoTime - lastTime).toDouble / 1000000000
        lastTime = System.nanoTime
        if (args.runtimeMeasurements) {
          print(s"$elapsed,")
        } else {
          println("\n-------------------------\n")
          println(s"New batch: $batchSize tweets")
          println(s"Processing time: $elapsed s")
          rdd.foreach {
            case (clusterId, (count, (silhouette, intra, inter), representative, url, text, interesting)) =>
              println(s"clusterId: $clusterId count: $count, silhouette: $silhouette, " +
                s"intra-distance: $intra, inter-distance: $inter, " +
                s"representative: $representative, interesting: $interesting, url: $url")
          }
        }
      } else if (args.runtimeMeasurements) System.exit(0)
    })
  }

  def execute() {

    createStreamingContext()

    // set log level
    LogManager.getRootLogger.setLevel(Level.OFF)

    // create Twitter Stream (tweet id, tweet text)
    val tweetIdTextStream: DStream[(Long, (String, Array[String]))]  = TweetStream.create(this)
    createKMeansModel()

    // preprocess tweets and create vectors
    val tweetIdVectorsStream: DStream[(Long, Vector)] = this.preprocessTweets(tweetIdTextStream)

    // run Clustering Algorithm and retrieve clusters (tweet Id, cluster Id)
    val tweetIdClusterIdStream: DStream[(Long, Int)] = this.clusterTweets(tweetIdVectorsStream)

    // contains (tweetId, ((clusterId, (text, urls)), vector)
    val joinedStream = tweetIdClusterIdStream.join(tweetIdTextStream).join(tweetIdVectorsStream)

    // contains (clusterId, (count, silhouette, closestRepresentative, interesting))
    val clusterInfoStream = this.createClusterInfoStream(joinedStream)
    writeClusterInfo(clusterInfoStream)

    this.outputClusterInfos(clusterInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
