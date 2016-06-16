import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TweetSanitizer extends StopWordsRemover {

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val t = udf { terms: Seq[String] =>
      terms
        // filter out punctuation (except @) and unicode emojis
        .map(s => s.replaceAll("""[[\p{Punct}‘“’”—–]&&[^@]]""", "").replaceAll("[\\ud83c\\udc00-\\ud83c\\udfff]|[\\ud83d\\udc00-\\ud83d\\udfff]|[\\u2600-\\u27ff]", ""))
        // remove URLs, mentions, "rt" (retweet), empty strings & words that are cut off by character limit
        .filter(s => !(s.contains("http") || s.contains("…") || s.contains("@") || (s == "rt") || (s == "")))
    }

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

}
