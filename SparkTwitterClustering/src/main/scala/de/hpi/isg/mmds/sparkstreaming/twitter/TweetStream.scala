package de.hpi.isg.mmds.sparkstreaming.twitter

import java.io._

import de.hpi.isg.mmds.sparkstreaming.TwitterClustering
import de.hpi.isg.mmds.sparkstreaming.nlp.NLPPipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable
import scala.util.parsing.json.JSON

object TweetStream {

  var centroids = Array[org.apache.spark.mllib.linalg.Vector]()
  var host: TwitterClustering = null

  def create(host: TwitterClustering) = {
      this.host = host
      if (host.args.tweetSource == "disk") createFromDisk else createFromAPI
  }

  def createFromAPI: DStream[(Long, (String, Array[String]))] = {

    def getTextAndURLs(tweet: Status): (String, Array[String]) = {
      var text = tweet.getText
      var currentTweet = tweet
      while (currentTweet.getQuotedStatus != null) {
        currentTweet = currentTweet.getQuotedStatus
        text = text + " @QUOTED: " + currentTweet.getText
      }
      (text, currentTweet.getURLEntities.map(u => u.getExpandedURL))
    }

    def startStream: DStream[(Long, (String, Array[String]))] = {
      val tweets = TwitterUtils.createStream(host.ssc, None, TwitterFilterArray.getFilterArray)
      val englishTweets = tweets.filter(_.getLang == "en")
      englishTweets.map(tweet => (tweet.getId, getTextAndURLs(tweet)))
    }

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    val tmpStream = startStream
    tmpStream.foreachRDD(rdd => {
      if ((!rdd.isEmpty()) & (centroids.length < host.args.k)) {
        val tweetRDD = rdd.map { case (id, (text, urls)) => (id, text) }
        centroids ++= NLPPipeline(host.args.vectorDimensions).preprocess(tweetRDD).map(_._2).collect().distinct
      }
    })

    host.ssc.start()
    host.ssc.awaitTerminationOrTimeout(30000)
    host.ssc.stop()
    host.createStreamingContext()

    centroids = centroids.take(host.args.k)
    startStream
  }

  def createFromDisk: DStream[(Long, (String, Array[String]))] = {

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

    val rddQueue = new mutable.Queue[RDD[(Long, (String, Array[String]))]]()

    val maxBatchCount = host.args.maxBatchCount
    if (!host.args.runtimeMeasurements) println(s"Preparing $maxBatchCount batches.")
    var tweetTuples : Array[(Long, (String, Array[String]))] = null;

    val filename = "preparedTweets_" + host.args.tweetsPerBatch + "_" +  host.args.maxBatchCount
    val filenameTuple = filename + "_tuple"
    val filenameCentroid = filename + "_centroid"
    val f = new File(filenameTuple)
    val lastTime = System.nanoTime
    if (f.exists() && !f.isDirectory()) {
      // load from disk
      if (!host.args.runtimeMeasurements) println(s"Load prepared tweets from disk.")
      var inputStream = new ObjectInputStream(new FileInputStream(filenameTuple))
      tweetTuples = inputStream.readObject().asInstanceOf[Array[(Long, (String, Array[String]))]]
      inputStream = new ObjectInputStream(new FileInputStream(filenameCentroid))
      centroids = inputStream.readObject().asInstanceOf[Array[org.apache.spark.mllib.linalg.Vector]]
    } else {
      val rdd = host.ssc.sparkContext.textFile(host.args.inputPath)
      val tweets = rdd.take(host.args.tweetsPerBatch * host.args.maxBatchCount)
      tweetTuples = host.ssc.sparkContext.parallelize(tweets)
        .map(tweet => tupleFromJSONString(tweet)).collect()

      while (centroids.length < host.args.k) {
        val fullTweetRDD = host.ssc.sparkContext.makeRDD[(Long, (String, Array[String]))](tweetTuples.take(host.args.k - centroids.length))
        val tweetRDD = fullTweetRDD.map { case (id, (text, urls)) => (id, text) }
        centroids ++= NLPPipeline(host.args.vectorDimensions).preprocess(tweetRDD).map(_._2).collect().distinct
      }

      // write tweetTuples and centroids to disk
      new ObjectOutputStream(new FileOutputStream(filenameTuple)).writeObject(tweetTuples);
      new ObjectOutputStream(new FileOutputStream(filenameCentroid)).writeObject(centroids);
    }
    if (!host.args.runtimeMeasurements) println((System.nanoTime - lastTime).toDouble / 1000000000)

    val iterator = tweetTuples.grouped(host.args.tweetsPerBatch)
    while (iterator.hasNext) rddQueue.enqueue(host.ssc.sparkContext.makeRDD[(Long, (String, Array[String]))](iterator.next()))
    if (!host.args.runtimeMeasurements) println("Finished preparing batches.")

    host.ssc.queueStream(rddQueue, oneAtATime = true)
  }

}
