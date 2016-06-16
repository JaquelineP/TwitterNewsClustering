import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingKMeansExample {

  def main(args: Array[String]) {

    // initialize spark streaming context
    val batchDuration = 10
    val conf = new SparkConf().setMaster("local[5]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

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

//    tweets.foreachRDD(tweetRdd => {
//      tweetRdd.foreach(singleTweet => {
//        println(singleTweet.getText())
//      })
//    })


    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(4, 0.0)
    model.trainOn(vectors)
    model.latestModel().clusterCenters.foreach(println)

    ssc.start()
    ssc.awaitTermination()
  }
}
