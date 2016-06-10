import java.io.File

object TwitterAuthentication {

  def readCredentials() = {

    val stream = getClass.getResourceAsStream("/config.txt")
    val lines:Array[String] = scala.io.Source.fromInputStream(stream).getLines().toArray

    if (lines.length != 4)
      throw new Exception("Error parsing twitter credentials!")


    System.setProperty("twitter4j.oauth.consumerKey", lines(0))
    System.setProperty("twitter4j.oauth.consumerSecret",lines(1))
    System.setProperty("twitter4j.oauth.accessToken", lines(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", lines(3))

  }

}
