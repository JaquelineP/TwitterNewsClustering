import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Level


object StreamingKMeansExample {

  val VectorDimensions = 1000

  def main(args: Array[String]) {

    // initialize spark streaming context
    val batchDuration = 10
    val conf = new SparkConf().setMaster("local[5]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    // create Twitter Stream
    val tweets = TwitterUtils.createStream(ssc, None, TwitterFilterArray.getFilterArray())
    val englishTweets = tweets.filter(_.getLang() == "en")

    // transform DStream[Status] to DStream[Vector]
    val textStream: DStream[String] = englishTweets.map(tweet => tweet.getText())
    val vectors: DStream[Vector] = textStream.transform(tweetRdd => {
      NLPPipeline.preprocess(tweetRdd)
    })

    // perform kMeans
    val model = new StreamingKMeans()
      .setK(100)
      .setDecayFactor(1.0)
      .setRandomCenters(VectorDimensions, 1.0)
    model.trainOn(vectors)

    var assignments = Array.emptyIntArray
    val assignmentStream = model.predictOn(vectors)
    assignmentStream.foreachRDD(rdd => assignments ++= rdd.collect())

    var tweetArray = Array.empty[String]
    //textStream.foreachRDD(rdd => tweetArray ++= rdd.collect())
    textStream.foreachRDD(rdd => rdd.foreach(tweet => tweetArray +: tweet))

    println("size: " + tweetArray.size)

    (tweetArray, assignments).zipped.foreach(
      (tweet, assignment) => println("cluster mapping: " + assignment + ":   " + tweet)
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
