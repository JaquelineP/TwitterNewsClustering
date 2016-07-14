package de.hpi.isg.mmds.sparkstreaming

class TweetObj(val text: String, val urls: Array[String])
  extends (String, Array[String])(text, urls)

class Tweet(val id: Long, val content: TweetObj)
  extends (Long, TweetObj)(id, content)
