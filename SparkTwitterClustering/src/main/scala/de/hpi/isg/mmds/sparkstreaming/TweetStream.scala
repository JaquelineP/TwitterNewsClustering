package de.hpi.isg.mmds.sparkstreaming

import breeze.linalg.min
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._

import scala.util.parsing.json.JSON
import scala.collection.mutable

object TweetStream {

  def startFromAPI(ssc: StreamingContext): DStream[(Long, String)] = {

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    // create Twitter Stream
    val tweets = TwitterUtils.createStream(ssc, None, TwitterFilterArray.getFilterArray)
    val englishTweets = tweets.filter(_.getLang == "en")

    val stream: DStream[(Long, String)] = englishTweets.map(tweet => {
      (tweet.getId, tweet.getText)
    })

    stream
  }

  def startFromDisk(ssc: StreamingContext, inputPath: String): DStream[(Long, String)] = {

    val TweetsPerBatch = 100    // amount of tweets per batch, one batch will be processed per time interval specified for streaming
    val MaxBatchCount = 10      // amount of batches that are prepared; streaming will crash after all prepared batch are processed!

    // return tuple with ID & TEXT from tweet as JSON string
    def tupleFromJSONString(tweet: String) = {
      val json = JSON.parseFull(tweet)
      val tuple = (
        json.get.asInstanceOf[Map[String, Any]]("id").asInstanceOf[Double].toLong,
        json.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String]
        )
      tuple
    }

    val rdd = ssc.sparkContext.textFile(inputPath)
    val rddQueue = new mutable.Queue[RDD[(Long, String)]]()

    val batchCount = min((rdd.count() / TweetsPerBatch).toInt, MaxBatchCount)
    for (batch <- 1 to batchCount) {
      println(s"Preparing batch $batch / $batchCount")
      val tweetTuples = rdd.take(TweetsPerBatch).map(tweet => tupleFromJSONString(tweet))
      rddQueue.enqueue(ssc.sparkContext.makeRDD[(Long, String)](tweetTuples))
    }
    ssc.queueStream(rddQueue, oneAtATime = true)
  }

}
