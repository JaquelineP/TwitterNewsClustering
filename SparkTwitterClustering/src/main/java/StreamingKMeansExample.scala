import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Benjamin Reissaus on 27/05/16.
  */



object StreamingKMeansExample {

  def main(args: Array[String]) {
//    if (args.length != 5) {
//      System.err.println(
//        "Usage: StreamingKMeansExample " +
//          "<trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>")
//      System.exit(1)
//    }

    val trainingsDir = "~/IdeaProjects/SparkHelloWorld/training_data/"
    val testDir = "~/IdeaProjects/SparkHelloWorld/test_data/"
    val batchDuration = 2000
    val numClusters = 3
    val numDimensions = 4
    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    val trainingData = ssc.textFileStream(trainingsDir).map(Vectors.parse)
    val testData = ssc.textFileStream(testDir).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData)
    print(model.latestModel().clusterCenters)
//    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
