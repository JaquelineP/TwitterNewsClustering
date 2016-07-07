package de.hpi.isg.mmds.sparkstreaming

import breeze.linalg.max
import de.hpi.isg.mmds.sparkstreaming.nlp.NLPPipeline
import de.hpi.isg.mmds.sparkstreaming.twitter.TweetStream
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import HashAggregation.writeHashes

case class TwitterClustering(args: Main.MainArgs.type) {

  val model = new StreamingKMeans()
    .setK(args.k)
    .setDecayFactor(args.forgetfulness)
    .setRandomCenters(args.vectorDimensions, 0.0)

  def preprocess(tweetIdTextStream: DStream[(Long, (String, Array[String]))]) = {

    val tweetIdVectorsCollisionMapStream = tweetIdTextStream.transform(tweetRdd => {

      if (!tweetRdd.isEmpty())
        NLPPipeline(args.vectorDimensions).preprocess(tweetRdd.map { case (id, (text, urls)) => (id, text) })
      else
        tweetRdd.sparkContext.emptyRDD[(Long, Vector, Map[Int, Seq[String]])]
    })

    tweetIdVectorsCollisionMapStream
  }

  def cluster(tweetIdVectorsStream: DStream[(Long, Vector)]) = {

    val vectorsStream = tweetIdVectorsStream.map{ case (tweetId, vector) => vector }

    model.trainOn(vectorsStream)
    model.predictOnValues(tweetIdVectorsStream)
  }

  def aggregate(joinedStream: DStream[(Long, ((Int, (String, Array[String])), Vector))]) = {
    val model = this.model

    val aggregateStream = joinedStream.map {
      case (tweetId, ((clusterId, (text, urls)), vector)) =>
        val center = model.latestModel().clusterCenters(clusterId)
        (tweetId, (text, urls, clusterId, Vectors.sqdist(vector, center)))
    }
    aggregateStream
  }

  def createClusterInfoStream(aggregatedStream:  DStream[(Long, (String, Array[String], Int, Double))]) = {
    val model = this.model

    aggregatedStream
      // move around attributes to have clusterId as key
      .map{case (tweetId, (text, urls, clusterId, sqDist)) => (clusterId, (1, sqDist, tweetId, urls))}

      // group over clusterId with count, sum of distances & id of tweet with closest distance per cluster
      .reduceByKey{case ((countA, sqDistA, tweetIdA, urlsA), (countB, sqDistB, tweetIdB, urlsB))
    => (countA + countB, sqDistA + sqDistB, if (sqDistA >= sqDistB) tweetIdB else tweetIdA, urlsA ++ urlsB)}

      // calculate average distance to center from sum of distances and count
      .map{case (clusterId, (count, distanceSum, representative, urls)) => (clusterId, (count, distanceSum / count, representative, urls))}

      // determine most frequently occurring url
      .map{case (clusterId, (count, distanceAvg, representative, urls)) =>
      val urlGroups = urls.groupBy(identity).mapValues(_.length)
      val best_url = if (urlGroups.isEmpty) "none" else urlGroups.maxBy{case (url, occurrences) => occurrences}._1
      (clusterId, (count, distanceAvg, representative, best_url))}

      // calculate "kind-of" silhouette for every cluster
      .map{case (clusterId, (count, avgSqDist, representative, url)) =>
      val center = model.latestModel().clusterCenters(clusterId)
      // calculate distance to all other cluster centers, and choose lowest
      val neighborDistance = Vectors.sqdist(center,
        model.latestModel.clusterCenters.minBy(otherCenter =>
          if (otherCenter != center) Vectors.sqdist(center, otherCenter) else Double.MaxValue))
      val silhouette = (neighborDistance - avgSqDist) / max(neighborDistance, avgSqDist)
      (clusterId, (count, (silhouette, avgSqDist, neighborDistance), representative, url))}

      // sort by silhouette
      .transform(rdd => rdd.sortBy( { case (clusterId, (count, silhouette, representative, url)) => silhouette._1 }, ascending = false, 1))

      // mark clusters with more than 2 tweets and silhouette >= 0 as interesting
      .map{ case (clusterId, (count, silhouette, representative, url))
    => (clusterId, (count, silhouette, representative, url, (count >= 3) && (silhouette._1 >= 0)))}
  }

  def createTweetInfoStream(aggregatedStream: DStream[(Long, (String, Array[String], Int, Double))],
                            clusterInfoStream:  DStream[(Int, (Int, (Double, Double, Double), Long, String, Boolean))]) = {

    aggregatedStream
      // move around attributes to have clusterId as key
      .map{case (tweetId, (text, urls, clusterId, sqDist)) => (clusterId, (tweetId, text, sqDist))}

      // join with cluster info stream
      .join(clusterInfoStream)

      // filter out tweets that are not from interesting clusters
      .filter{case (clusterId, ((tweetId, text, sqDist), (count, silhouette, rep, url, interesting))) => interesting}

      // remove un-needed attributes again
      .map{case (clusterId, ((tweetId, text, sqDist), (count, silhouette, rep, url, interesting))) => (tweetId, (text, clusterId, sqDist))}
  }

  def outputClusterInfos(clusterInfoStream: DStream[(Int, (Int, (Double, Double, Double), Long, String, Boolean))], lastTime: Long) = {
    clusterInfoStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val batchSize = rdd.map {
          case (clusterId, (count, distanceSum, representative, url, interesting)) => count
        }.reduce(_+_)

        val elapsed = (System.nanoTime - lastTime).toDouble / 1000000000
//        lastTime = System.nanoTime
        if (args.runtimeMeasurements) {
          print(s"$elapsed,")
        } else {
          println("\n-------------------------\n")
          println(s"New batch: $batchSize tweets")
          println(s"Processing time: $elapsed s")
          rdd.foreach {
            case (clusterId, (count, (silhouette, intra, inter), representative, url, interesting)) =>
              println(s"clusterId: $clusterId count: $count, silhouette: $silhouette, " +
                s"intra-distance: $intra, inter-distance: $inter, " +
                s"representative: $representative, interesting: $interesting, url: $url")
          }
        }
      } else if (args.runtimeMeasurements) System.exit(0)
    })
  }

  def outputTweetInfoStream(tweetInfoStream: DStream[(Long, (String, Int, Double))]) = {
    tweetInfoStream.foreachRDD(rdd => {
      if (!args.runtimeMeasurements) {
        println("\n-------------------------\n")
        rdd.foreach {
          case (tweetId, (text, clusterId, sqDist)) =>
            println(s"tweetId: $tweetId clusterId: $clusterId, text: $text, sqDist: $sqDist")
        }
      }
    })
  }


  def execute() {

    // initialize spark streaming context
    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(args.batchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // create Twitter Stream (tweet id, tweet text)
    val tweetIdTextStream = TweetStream.create(ssc, args.tweetSource, args.inputPath, args.tweetsPerBatch, args.maxBatchCount, args.runtimeMeasurements)

    val lastTime = System.nanoTime

    // preprocess tweets and create vectors
    val tweetIdVectorsCollisionMapStream = this.preprocess(tweetIdTextStream)

    //TODO: kann das direkt im preprocess gemacht werden?
    writeHashes(tweetIdVectorsCollisionMapStream)

    val tweetIdVectorsStream = tweetIdVectorsCollisionMapStream.map {
      case (tweetId, vector, hashWordList) => (tweetId, vector)
    }

    // Run Clustering Algorithm and retrieve clusters (tweet Id, cluster Id)
    val tweetIdClusterIdStream = this.cluster(tweetIdVectorsStream)

    // contains (tweetId, ((clusterId, (text, urls)), vector)
    val joinedStream = tweetIdClusterIdStream.join(tweetIdTextStream).join(tweetIdVectorsStream)

    // contains (tweetId, (text, urls, clusterId, sqDist))
    val aggregatedStream = this.aggregate(joinedStream)

    // contains (clusterId, (count, silhouette, closestRepresentative, interesting))
    val clusterInfoStream = this.createClusterInfoStream(aggregatedStream)

    // contains (tweetId, (text, clusterId, sqDist)) for tweets from interesting clusters
    val tweetInfoStream = this.createTweetInfoStream(aggregatedStream, clusterInfoStream)

    this.outputClusterInfos(clusterInfoStream, lastTime)
    this.outputTweetInfoStream(tweetInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
