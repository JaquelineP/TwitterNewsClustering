package de.hpi.isg.mmds.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg._
object HashAggregation {


  def main(args: Array[String]): Unit = {
    aggregate()
  }

  def aggregate() = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansExample")
    val sc = new SparkContext(conf)

    val hashValues: RDD[(Int, Seq[String])] = sc.objectFile("output/batch_collisions/batch-*")

    hashValues
      .reduceByKey {
        case (v1, v2) => Seq.concat(v1,v2).distinct
      }
      .filter {
        case (hash, values) => values.length > 1
      }
      .sortByKey()
      .saveAsTextFile("output/merged_collisions")

  }

  def writeHashes(outputStream: DStream[(Long, Vector, Map[Int, Seq[String]])]) = {

    outputStream
      .flatMap {
        case (twitterId: Long, vector: Vector, collisionMap: Map[Int, Seq[String]]) => {
          collisionMap
        }
      }
      .reduceByKey {
        case (v1, v2) => Seq.concat(v1,v2).distinct
      }
      .saveAsObjectFiles("output/batch_collisions/batch")
  }

}
