package de.hpi.isg.mmds.sparkstreaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKMeansExample {

  val VectorDimensions = 1000     // amount of dimensions for vectorizing tweets
  val BatchDuration = 10          // batch duration in seconds

  def main(args: Array[String]) {

    // initialize spark streaming context
    val conf = new SparkConf().setMaster("local[5]").setAppName("StreamingKMeansExample")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(BatchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // start streaming from specified source (startFromAPI: API; startFromDisk: file on disc)
    val tweetIdTextStream: DStream[(Long, String)] = TweetStream.startFromAPI(ssc)

    // preprocess tweets with NLP pipeline
    val tweetIdVectorsStream: DStream[(Long, Vector)] = tweetIdTextStream.transform(tweetRdd => {
      NLPPipeline.preprocess(tweetRdd)
    })

    val vectorsStream: DStream[Vector] = tweetIdVectorsStream.map{ case (tweetId, vector) => vector }

    // perform kMeans
    val model = new StreamingKMeans()
      .setK(100)
      .setDecayFactor(1.0)
      .setRandomCenters(VectorDimensions, 1.0)
      model.trainOn(vectorsStream)

    val tweetIdClusterIdStream = model.predictOnValues(tweetIdVectorsStream)
    val tweetIdWithTextAndClusterIdStream = tweetIdClusterIdStream.join(tweetIdTextStream)

    tweetIdWithTextAndClusterIdStream.foreachRDD(rdd => {
      rdd.foreach{ case(tweetId, (clusterId, text)) => println(s"tweetId: $tweetId clusterId: $clusterId, text: $text")}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
