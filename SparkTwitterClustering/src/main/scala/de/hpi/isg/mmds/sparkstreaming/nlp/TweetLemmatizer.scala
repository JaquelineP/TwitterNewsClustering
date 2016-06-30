package de.hpi.isg.mmds.sparkstreaming.nlp

import java.io.{PipedOutputStream, PrintStream}
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class TweetLemmatizer extends Tokenizer {

  def plainTextToLemmas(text: String): Seq[String] = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = doc.get(classOf[TokensAnnotation]).map(_.get(classOf[LemmaAnnotation]))
    lemmas
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    // remove output of NLP lemmatizer
    System.setErr(new PrintStream(new PipedOutputStream()))

    val t = udf { terms: String => plainTextToLemmas(terms)}
    System.setErr(System.err)

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

}
