package de.hpi.isg.mmds.sparkstreaming

import breeze.linalg.max
import de.hpi.isg.mmds.sparkstreaming.nlp.NLPPipeline
import de.hpi.isg.mmds.sparkstreaming.twitter.TweetStream
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConverters._
import HashAggregation.writeHashes

object TwitterArgs {

  @Option(name = "-input", required = true,
    usage = "path to input file")
  var inputPath: String = null

  @Option(name = "-k",
    usage = "k parameter for K-Means")
  var k: Int = 100

  @Option(name = "-decay",
    usage = "forgetfulness factor")
  var forgetfulness: Double = 0.7

  @Option(name = "-tweetsPerBatch",
    usage = "amount of tweets per batch")
  var TweetsPerBatch: Int = 100

  @Option(name = "-maxBatchCount",
    usage = "amount of batches that are prepared, default value 10")
  var MaxBatchCount: Int = 10

  @Option(name = "-dimensions",
    usage = "dimensions for vectorization, default value 1000")
  var VectorDimensions: Int = 1000

  @Option(name = "-batchDuration",
    usage = "batch duration in seconds, default value 10")
  var BatchDuration: Int = 10

  @Option(name = "-source",
    usage = "source for tweets, either 'disk' (default) or 'api'")
  var TweetSource: String = "disk"

  @Option(name = "-runtime",
    usage = "weather only run times are printed")
  var RuntimeMeasurements: Boolean = false
}

object StreamingKMeansExample {

  def main(args: Array[String]) {

    val parser = new CmdLineParser(TwitterArgs)
    try {
      parser.parseArgument(args.toList.asJava)
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }

    // initialize spark streaming context
    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(TwitterArgs.BatchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // start streaming from specified source (startFromAPI: API; startFromDisk: file on disc)
    val tweetIdTextStream: DStream[(Long, (String, Array[String]))] =
      if (TwitterArgs.TweetSource == "disk")
        TweetStream.startFromDisk(ssc, TwitterArgs.inputPath, TwitterArgs.TweetsPerBatch, TwitterArgs.MaxBatchCount)
      else
        TweetStream.startFromAPI(ssc)

    var lastTime = System.nanoTime

    // preprocess tweets with NLP pipeline
    val tweetIdVectorsCollisionMapStream: DStream[(Long, Vector, Map[Int, Seq[String]])] = tweetIdTextStream.transform(tweetRdd => {

      if (!tweetRdd.isEmpty())
        NLPPipeline.preprocess(tweetRdd.map{case (id, (text, urls)) => (id, text)})
      else
        tweetRdd.sparkContext.emptyRDD[(Long, Vector, Map[Int, Seq[String]])]
    })

    writeHashes(tweetIdVectorsCollisionMapStream)


    val tweetIdVectorsStream = tweetIdVectorsCollisionMapStream.map { case (tweetId, vector, hashWordList) => {
      //println(s"$tweetId, $vector")
      (tweetId, vector)
    }}

    val vectorsStream: DStream[Vector] = tweetIdVectorsStream.map{ case (tweetId, vector) => vector }

    // perform kMeans
    val model = new StreamingKMeans()
      .setK(TwitterArgs.k)
      .setDecayFactor(TwitterArgs.forgetfulness)
      .setRandomCenters(TwitterArgs.VectorDimensions, 0.0)
    model.trainOn(vectorsStream)

    val tweetIdClusterIdStream = model.predictOnValues(tweetIdVectorsStream)

    // contains (tweetId, (((text, urls), clusterId), vector))
    val joinedStream = tweetIdClusterIdStream.join(tweetIdTextStream).join(tweetIdVectorsStream)

    // contains (tweetId, (text, urls, clusterId, sqDist))
    val aggregateStream = joinedStream.map {
      case (tweetId, ((clusterId, (text, urls)), vector)) =>
        val center = model.latestModel().clusterCenters(clusterId)
        (tweetId, (text, urls, clusterId, Vectors.sqdist(vector, center)))
    }

    // contains (clusterId, (count, silhouette, closestRepresentative, interesting))
    val clusterInfoStream = aggregateStream

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
        (clusterId, (count, (neighborDistance - avgSqDist) / max(neighborDistance, avgSqDist), representative, url))}

      // sort by silhouette
      .transform(rdd => rdd.sortBy( { case (clusterId, (count, silhouette, representative, url)) => silhouette }, ascending = false, 1))

      // mark clusters with more than 2 tweets and silhouette >= 0 as interesting
      .map{ case (clusterId, (count, silhouette, representative, url))
        => (clusterId, (count, silhouette, representative, url, (count >= 3) && (silhouette >= 0)))}

    // contains (tweetId, (text, clusterId, sqDist)) for tweets from interesting clusters
    val tweetInfoStream = aggregateStream

      // move around attributes to have clusterId as key
      .map{case (tweetId, (text, urls, clusterId, sqDist)) => (clusterId, (tweetId, text, sqDist))}

      // join with cluster info stream
      .join(clusterInfoStream)

      // filter out tweets that are not from interesting clusters
      .filter{case (clusterId, ((tweetId, text, sqDist), (count, silhouette, rep, url, interesting))) => interesting}

      // remove un-needed attributes again
      .map{case (clusterId, ((tweetId, text, sqDist), (count, silhouette, rep, url, interesting))) => (tweetId, (text, clusterId, sqDist))}

    clusterInfoStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val batchSize = rdd.map {
          case (clusterId, (count, distanceSum, representative, url, interesting)) => count
        }.reduce(_+_)

        val elapsed = (System.nanoTime - lastTime).toDouble / 1000000000
        lastTime = System.nanoTime
        if (!TwitterArgs.RuntimeMeasurements) {
          println("\n-------------------------\n")
          println(s"New batch: $batchSize tweets")
          println(s"Processing time: $elapsed s")
        } else {
          print(s"$elapsed,")
        }
        /*rdd.foreach {
          case (clusterId, (count, distanceSum, representative, url, interesting)) =>
            println(s"clusterId: $clusterId count: $count, silhouette: $distanceSum, " +
              s"representative: $representative, interesting: $interesting, url: $url")
        }*/
      }
    })

    tweetInfoStream.foreachRDD(rdd => {
      /*println("\n-------------------------\n")
      rdd.foreach{
        case(tweetId, (text, clusterId, sqDist)) =>
          println(s"tweetId: $tweetId clusterId: $clusterId, text: $text, sqDist: $sqDist")
      }*/

      // convert RDD to dataframe
      val sqlContext = new SQLContext(rdd.sparkContext)
      import sqlContext.implicits._

      val clusterResults = rdd.toDF()
      clusterResults.registerTempTable("clusterresults")

      //      val my_df = sqlContext.sql("SELECT * from clusterresults LIMIT 5")
      //      my_df.collect().foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
