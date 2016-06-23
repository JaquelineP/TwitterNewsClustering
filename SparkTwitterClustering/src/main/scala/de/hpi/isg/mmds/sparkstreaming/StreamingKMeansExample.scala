package de.hpi.isg.mmds.sparkstreaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors

object StreamingKMeansExample {

  val VectorDimensions = 1000     // amount of dimensions for vectorizing tweets
  val BatchDuration = 10          // batch duration in seconds

  def main(args: Array[String]) {


    if (args.isEmpty) {
      println("Usage: scala <main class> <input URL>")
      sys.exit(1)
    }

    var inputPath = args(0)

    // initialize spark streaming context
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(BatchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // start streaming from specified source (startFromAPI: API; startFromDisk: file on disc)
    val tweetIdTextStream: DStream[(Long, String)] = TweetStream.startFromDisk(ssc, inputPath)

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
      .setK(100)
      .setDecayFactor(1.0)
      .setRandomCenters(VectorDimensions, 1.0)
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
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, if (a._2 >= b._2) a._3 else b._3))
      .map{case (clusterId, (count, distanceSum, representative)) => (clusterId, (count, distanceSum / count, representative))}


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
