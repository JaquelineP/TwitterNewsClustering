package de.hpi.isg.mmds.sparkstreaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingKMeansExample {

  val VectorDimensions = 1000

  def main(args: Array[String]) {

    // initialize spark streaming context
    val batchDuration = 10
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    // set log level
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // Twitter Authentication
    TwitterAuthentication.readCredentials()

    // create Twitter Stream
    val tweets = TwitterUtils.createStream(ssc, None, TwitterFilterArray.getFilterArray())
    val englishTweets = tweets.filter(_.getLang() == "en")

    val tweetIdTextStream: DStream[(Long, String)] = englishTweets.map(tweet => {
      (tweet.getId(),tweet.getText())
    })

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

    val clusterIdCount = tweetIdClusterIdStream.map {
      case (tweetId, clusterId) => (clusterId, 1)
    }
      .reduceByKey(_+_)


    clusterIdCount.foreachRDD(rdd => {
      val batchSize = rdd.map {
        case (clusterId, count) => count
      }.reduce(_+_)

      println(s"New Batch: $batchSize")
      rdd.foreach {
        case (clusterId, count) => {
          println( s"clusterId: $clusterId count: $count")
        }
      }
      println("-------------------------")
    })




//    tweetIdWithTextAndClusterIdStream.foreachRDD(rdd => {
//      rdd.foreach{
//        case(tweetId, (clusterId, text)) => {
//          println(s"tweetId: $tweetId clusterId: $clusterId, text: $text")
//        }
//      }
//
//      // convert RDD to dataframe
//      val sqlContext = new SQLContext(rdd.sparkContext)
//      import sqlContext.implicits._
//
//      val clusterResults = rdd.toDF()
//      clusterResults.registerTempTable("clusterresults")
//
//      //      val my_df = sqlContext.sql("SELECT * from clusterresults LIMIT 5")
//      //      my_df.collect().foreach(println)
//    })


    ssc.start()
    ssc.awaitTermination()
  }
}
