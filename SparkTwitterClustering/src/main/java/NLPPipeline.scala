import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IDF, HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg._

object NLPPipeline {

  val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")

  val remover = new StopWordsRemover()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("filtered")

  val hashingTF = new HashingTF()
    .setNumFeatures(1000)

    .setInputCol(remover.getOutputCol)
    .setOutputCol("tf")

  val inverseDocumentFreq = new IDF()
    .setInputCol(hashingTF.getOutputCol)
    .setOutputCol("idf")

  //TODO: add stemming here
  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, remover, hashingTF, inverseDocumentFreq))


  def preprocess(tweets: RDD[String]): RDD[Vector] = {

    // convert RDD to dataframe
    val sqlContext = new SQLContext(tweets.sparkContext)
    import sqlContext.implicits._

    // the new column's name is called 'text' --> tokenizer needs it
    val dataframe = tweets.toDF("text")

    // apply NLP Pipeline
    val pipelineModel = pipeline.fit(dataframe)
    val vectorsDataFrame = pipelineModel.transform(dataframe)

    // convert back to RDD
    val assembled: RDD[Vector] = vectorsDataFrame.map(row => row.getAs[Vector]("tf").toDense)
    assembled
  }

}
