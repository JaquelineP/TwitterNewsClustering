package de.hpi.isg.mmds.sparkstreaming.nlp

import de.hpi.isg.mmds.sparkstreaming.StreamingKMeansExample
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object NLPPipeline {

  val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")

  val sanitizer = new TweetSanitizer()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("sanitizedWords")

  val remover = new StopWordsRemover()
    .setInputCol(sanitizer.getOutputCol)
    .setOutputCol("filtered")

  val hashingTF = new HashingTF()
    .setNumFeatures(StreamingKMeansExample.VectorDimensions)
    .setInputCol(remover.getOutputCol)
    .setOutputCol("tf")

  val inverseDocumentFreq = new IDF()
    .setInputCol(hashingTF.getOutputCol)
    .setOutputCol("idf")

  //TODO: add stemming here
  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, sanitizer, remover, hashingTF, inverseDocumentFreq))


  def preprocess(tweets: RDD[(Long, String)]): RDD[(Long, Vector)] = {

    // convert RDD to dataframe
    val sqlContext = new SQLContext(tweets.sparkContext)
    import sqlContext.implicits._

    // the new column's name is called 'text' --> tokenizer needs it
    val dataframe = tweets.toDF("key", "text")

    // apply NLP Pipeline
    val pipelineModel = pipeline.fit(dataframe)
    val vectorsDataFrame = pipelineModel.transform(dataframe)

    // convert back to RDD
    val assembled: RDD[(Long, Vector)] = vectorsDataFrame.map(row => (row.getAs[Long]("key"), row.getAs[Vector]("idf").toDense))
    assembled
  }

}
