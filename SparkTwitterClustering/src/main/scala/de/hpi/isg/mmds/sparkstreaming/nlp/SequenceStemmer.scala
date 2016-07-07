package de.hpi.isg.mmds.sparkstreaming.nlp

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.tartarus.snowball.SnowballStemmer

class SequenceStemmer extends StopWordsRemover {

  val stemClass = Class.forName("org.tartarus.snowball.ext.englishStemmer")
  val stemmer = stemClass.newInstance.asInstanceOf[SnowballStemmer]

  def stem(word: String): String = {
    try {
      stemmer.setCurrent(word)
      stemmer.stem()
      stemmer.getCurrent
    } catch {
      case e: Exception => word
    }
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val t = udf { terms: Seq[String] => terms.map(stem(_)) }

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

}
