package de.hpi.isg.mmds.sparkstreaming.nlp

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class NLPPipeline(vectorDimensions: Int) {

  val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("tokenizedText")

  val stemmer = new SequenceStemmer()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("stemmedText")

  val sanitizer = new TweetSanitizer()
    .setInputCol(stemmer.getOutputCol)
    .setOutputCol("sanitizedWords")

  val remover = new StopWordsRemover()
    .setInputCol(sanitizer.getOutputCol)
    .setOutputCol("filtered")

  val hashingTF = new HashingTF()
    .setNumFeatures(this.vectorDimensions)
    .setInputCol(remover.getOutputCol)
    .setOutputCol("tf")

  val inverseDocumentFreq = new IDF()
    .setInputCol(hashingTF.getOutputCol)
    .setOutputCol("idf")

  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, stemmer, sanitizer, remover, hashingTF, inverseDocumentFreq))


  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def indexOf(term: Any): Int = this.nonNegativeMod(term.##, this.vectorDimensions)

  val collisionMap = udf((words: Seq[String]) => {
    val collisionMap = mutable.HashMap.empty[Int, Seq[String]]

    words.map(word => {

      val hash = this.indexOf(word)
      val collisionVector = collisionMap.getOrElse[Seq[String]](hash, Seq[String]())

      val newCollisionVector = (collisionVector :+ word).distinct
      collisionMap.put(hash, newCollisionVector)
    })
    collisionMap
  })

  def preprocess(tweets: RDD[(Long, String)]): RDD[(Long, Vector, Map[Int, Seq[String]])] = {

    // convert RDD to dataframe
    val sqlContext = new SQLContext(tweets.sparkContext)
    import sqlContext.implicits._

    // the new column's name is called 'text' --> tokenizer needs it
    val dataframe = tweets.toDF("key", "text")

    // apply NLP Pipeline
    val pipelineModel = pipeline.fit(dataframe)
    val vectorsDataFrame = pipelineModel.transform(dataframe)

    val changedDataFrame = vectorsDataFrame.withColumn("collisionMap", collisionMap(vectorsDataFrame("filtered")))

    // convert back to RDD
    val assembled: RDD[(Long, Vector, Map[Int, Seq[String]])] = changedDataFrame.map(row => (row.getAs[Long]("key"), row.getAs[Vector]("idf").toDense, row.getAs[Map[Int, Seq[String]]]("collisionMap")))
    assembled
  }

}
