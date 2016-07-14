package de.hpi.isg.mmds.sparkstreaming

class Cluster(val score: Score, val interesting: Boolean, val representative: Tweet, val best_url: String, val fixed_id: Int)
  extends (Score, Boolean, Tweet, String, Int)(score, interesting, representative, best_url, fixed_id)

class Score(val count: Int, val silhouette: Double, val inter: Double, val intra: Double)
  extends (Int, Double, Double, Double)(count, silhouette, inter, intra)