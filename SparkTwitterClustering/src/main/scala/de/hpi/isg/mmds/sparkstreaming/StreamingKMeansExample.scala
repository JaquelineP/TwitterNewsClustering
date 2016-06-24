package de.hpi.isg.mmds.sparkstreaming

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
}

object StreamingKMeansExample {

  val VectorDimensions = 1000     // amount of dimensions for vectorizing tweets
  val BatchDuration = 10          // batch duration in seconds

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(BatchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // start streaming from specified source (startFromAPI: API; startFromDisk: file on disc)
    val tweetIdTextStream: DStream[(Long, String)] = TweetStream.startFromDisk(ssc, TwitterArgs.inputPath, TwitterArgs.TweetsPerBatch, TwitterArgs.MaxBatchCount)

    // preprocess tweets with NLP pipeline
    val tweetIdVectorsStream: DStream[(Long, Vector)] = tweetIdTextStream.transform(tweetRdd => {

      if (!tweetRdd.isEmpty())
        NLPPipeline.preprocess(tweetRdd)
      else
        tweetRdd.sparkContext.emptyRDD[(Long, Vector)]
    })

    val vectorsStream: DStream[Vector] = tweetIdVectorsStream.map{ case (tweetId, vector) => vector }

    // perform kMeans
    val model = new StreamingKMeans()
      .setK(TwitterArgs.k)
      .setDecayFactor(TwitterArgs.forgetfulness)
      .setRandomCenters(VectorDimensions, 0.0)
    model.trainOn(vectorsStream)

    val tweetIdClusterIdStream = model.predictOnValues(tweetIdVectorsStream)

    // contains (tweetId, ((text, clusterId), vector))
    val joinedStream = tweetIdClusterIdStream.join(tweetIdTextStream).join(tweetIdVectorsStream)

    // contains (tweetId, (text, clusterId, sqDist))
    val aggregateStream = joinedStream.map {
      case (tweetId, ((clusterId, text), vector)) =>
        val center = model.latestModel().clusterCenters(clusterId)
        (tweetId, (text, clusterId, Vectors.sqdist(vector, center)))
    }

    // contains (clusterId, (count, avgSqDist, closestRepresentative))
    val clusterInfoStream = aggregateStream
      .map{case (tweetId, (text, clusterId, sqDist)) => (clusterId, (1, sqDist, tweetId))}
      .reduceByKey{case ((countA, sqDistA, tweetIdA), (countB, sqDistB, tweetIdB))
        => (countA + countB, sqDistA + sqDistB, if (sqDistA >= sqDistB) tweetIdB else tweetIdA)}
      .map{case (clusterId, (count, distanceSum, representative)) => (clusterId, (count, distanceSum / count, representative))}
      .transform(rdd => rdd.sortBy( { case (clusterId, (count, sqDistAvg, representative)) => sqDistAvg }, true, 1))


    clusterInfoStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val batchSize = rdd.map {
          case (clusterId, (count, distanceSum, representative)) => count
        }.reduce(_+_)

        println("\n-------------------------\n")
        println(s"New batch: $batchSize tweets")
        rdd.foreach {
          case (clusterId, (count, distanceSum, representative)) =>
            println(s"clusterId: $clusterId count: $count, average distance: $distanceSum, representative: $representative")
        }
      }
    })

    aggregateStream.foreachRDD(rdd => {
      println("\n-------------------------\n")
      rdd.foreach{
        case(tweetId, (text, clusterId, sqDist)) =>
          println(s"tweetId: $tweetId clusterId: $clusterId, text: $text, sqDist: $sqDist")
      }

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
