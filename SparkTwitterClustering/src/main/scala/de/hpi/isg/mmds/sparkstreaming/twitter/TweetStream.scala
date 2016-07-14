package de.hpi.isg.mmds.sparkstreaming.twitter

import java.io._

import de.hpi.isg.mmds.sparkstreaming.{Tweet, TweetObj, TwitterClustering}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable
import scala.util.parsing.json.JSON

object TweetStream {

  var host: TwitterClustering = null

  def create(host: TwitterClustering) = {
      this.host = host
      if (host.args.tweetSource == "disk") createFromDisk else createFromAPI
  }

  def createFromAPI: DStream[Tweet] = {

    def getTextAndURLs(tweet: Status): TweetObj = {
      var text = tweet.getText
      var currentTweet = tweet
      while (currentTweet.getQuotedStatus != null) {
        currentTweet = currentTweet.getQuotedStatus
        text = text + " @QUOTED: " + currentTweet.getText
      }
      new TweetObj(text, currentTweet.getURLEntities.map(u => u.getExpandedURL))
    }

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    val tweets = TwitterUtils.createStream(host.ssc, None, TwitterFilterArray.getFilterArray)
    val englishTweets = tweets.filter(_.getLang == "en")
    englishTweets.map { case tweet =>
      new Tweet(tweet.getId, getTextAndURLs(tweet))
    }
  }

  def createFromDisk: DStream[Tweet] = {

    // return tuple with ID & TEXT from tweet as JSON string
    def tupleFromJSONString(tweet: String) = {
      val json = JSON.parseFull(tweet)
      val urlString = json.get.asInstanceOf[Map[String, Any]]("urls").asInstanceOf[String]
      val urls = if (urlString == null) Array.empty[String] else urlString.split(",")
      new Tweet ( json.get.asInstanceOf[Map[String, Any]]("id").asInstanceOf[Double].toLong,
        new TweetObj(json.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String], urls))
    }

    val rddQueue = new mutable.Queue[RDD[Tweet]]()

    val maxBatchCount = host.args.maxBatchCount
    if (!host.args.runtimeMeasurements) println(s"Preparing $maxBatchCount batches.")
    var tweetTuples : Array[Tweet] = null

    val filename = "preparedTweets_" + host.args.tweetsPerBatch + "_" +  host.args.maxBatchCount
    val f = new File(filename)
    if (f.exists() && !f.isDirectory) {
      // load from disk
      if (!host.args.runtimeMeasurements) println(s"Load prepared tweets from disk.")
      val inputStream = new ObjectInputStream(new FileInputStream(filename))
      tweetTuples = inputStream.readObject().asInstanceOf[Array[Tweet]]
    } else {
      val rdd = host.ssc.sparkContext.textFile(host.args.inputPath)
      val tweets = rdd.take(host.args.tweetsPerBatch * host.args.maxBatchCount)
      tweetTuples = host.ssc.sparkContext.parallelize(tweets)
        .map{case tweet =>
          tupleFromJSONString(tweet)
        }.collect()

      // write tweetTuples to disk
      new ObjectOutputStream(new FileOutputStream(filename)).writeObject(tweetTuples)
    }

    val iterator = tweetTuples.grouped(host.args.tweetsPerBatch)
    while (iterator.hasNext) rddQueue.enqueue(host.ssc.sparkContext.makeRDD[Tweet](iterator.next()))
    if (!host.args.runtimeMeasurements) println("Finished preparing batches.")

    host.ssc.queueStream(rddQueue, oneAtATime = true)
  }

}
