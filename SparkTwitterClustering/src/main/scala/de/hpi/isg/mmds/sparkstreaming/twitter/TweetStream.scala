package de.hpi.isg.mmds.sparkstreaming.twitter

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable
import scala.util.parsing.json.JSON

object TweetStream {

  def create(ssc: StreamingContext, streamSource: String, inputPath: String, tweetsPerBatch: Int, maxBatchCount: Int, runtimeMeasurements: Boolean) = {

      if (streamSource == "disk")
        TweetStream.createFromDisk(ssc, inputPath, tweetsPerBatch, maxBatchCount, runtimeMeasurements)
      else
        TweetStream.createFromAPI(ssc)
  }

  def createFromAPI(ssc: StreamingContext): DStream[(Long, (String, Array[String]))] = {

    def getTextAndURLs(tweet: Status): (String, Array[String]) = {
      var text = tweet.getText
      var currentTweet = tweet
      while (currentTweet.getQuotedStatus != null) {
        currentTweet = currentTweet.getQuotedStatus
        text = text + " @QUOTED: " + currentTweet.getText
      }
      (text, currentTweet.getURLEntities.map(u => u.getExpandedURL))
    }

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    // create Twitter Stream
    val tweets = TwitterUtils.createStream(ssc, None, TwitterFilterArray.getFilterArray)
    val englishTweets = tweets.filter(_.getLang == "en")

    val stream: DStream[(Long, (String, Array[String]))] = englishTweets.map(tweet => {
      (tweet.getId, getTextAndURLs(tweet))
    })

    stream
  }

  /**
    * Starts the Stream reading from disk.
 *
    * @param ssc spark context
    * @param inputPath path to resources
    * @param TweetsPerBatch optional defaultValue = 100, amount of tweets per batch, one batch will be processed per time interval specified for streaming
    * @param MaxBatchCount optional defaultValue = 10, amount of batches that are prepared; streaming will crash after all prepared batch are processed!
    * @return
    */
  def createFromDisk(ssc: StreamingContext, inputPath: String, TweetsPerBatch: Int = 100, MaxBatchCount: Int = 10,
                     RuntimeMeasurements: Boolean = false): DStream[(Long, (String, Array[String]))] = {

    // return tuple with ID & TEXT from tweet as JSON string
    def tupleFromJSONString(tweet: String) = {
      val json = JSON.parseFull(tweet)
      val urlString = json.get.asInstanceOf[Map[String, Any]]("urls").asInstanceOf[String]
      val urls = if (urlString == null) Array.empty[String] else urlString.split(",")
      val tuple = (
        json.get.asInstanceOf[Map[String, Any]]("id").asInstanceOf[Double].toLong,
        (json.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String], urls)
        )
      tuple
    }

    val rdd = ssc.sparkContext.textFile(inputPath)
    val rddQueue = new mutable.Queue[RDD[(Long, (String, Array[String]))]]()

    if (!RuntimeMeasurements) println(s"Preparing $MaxBatchCount batches.")
    val tweets = rdd.take(TweetsPerBatch * MaxBatchCount)
    val tweetTuples = ssc.sparkContext.parallelize(tweets).map(tweet => tupleFromJSONString(tweet)).collect()
    val iterator = tweetTuples.grouped(TweetsPerBatch)
    while (iterator.hasNext) rddQueue.enqueue(ssc.sparkContext.makeRDD[(Long, (String, Array[String]))](iterator.next()))
    if (!RuntimeMeasurements) println("Finished preparing batches.")

    ssc.queueStream(rddQueue, oneAtATime = true)
  }

}
