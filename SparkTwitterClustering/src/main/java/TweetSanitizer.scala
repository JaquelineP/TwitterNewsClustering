package org.apache.spark.ml.feature

import org.apache.spark.annotation.{Since, Experimental}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

class TweetSanitizer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("tweetSanitizer"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

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

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
    SchemaUtils.appendColumn(schema, $(outputCol), inputType, schema($(inputCol)).nullable)
  }

  override def copy(extra: ParamMap): TweetSanitizer = defaultCopy(extra)
}

object TweetSanitizer extends DefaultParamsReadable[TweetSanitizer] {

  override def load(path: String): TweetSanitizer = super.load(path)
}
