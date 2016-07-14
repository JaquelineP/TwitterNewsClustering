package de.hpi.isg.mmds.sparkstreaming

class Cluster(val score: Score, val interesting: Boolean, val representative: Tweet, val best_url: String)
  extends (Score, Boolean, Tweet, String)(score, interesting, representative, best_url)

class Score(val count: Int, val silhouette: Double, val inter: Double, val intra: Double)
  extends (Int, Double, Double, Double)(count, silhouette, inter, intra)