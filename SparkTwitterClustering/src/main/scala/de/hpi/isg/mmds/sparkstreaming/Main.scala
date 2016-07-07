package de.hpi.isg.mmds.sparkstreaming

import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConverters._

object Main {

  object MainArgs {

    @Option(name = "-input", required = true,
      usage = "path to input file")
    var inputPath: String = null

    @Option(name = "-k",
      usage = "k parameter for K-Means")
    var k: Int = 100

    @Option(name = "-decay",
      usage = "forgetfulness factor")
    var forgetfulness: Double = 0.7

    @Option(name = "-tweetsPerBatch",
      usage = "amount of tweets per batch")
    var tweetsPerBatch: Int = 100

    @Option(name = "-maxBatchCount",
      usage = "amount of batches that are prepared, default value 10")
    var maxBatchCount: Int = 10

    @Option(name = "-dimensions",
      usage = "dimensions for vectorization, default value 1000")
    var vectorDimensions: Int = 1000

    @Option(name = "-batchDuration",
      usage = "batch duration in seconds, default value 10")
    var batchDuration: Int = 10

    @Option(name = "-source",
      usage = "source for tweets, either 'disk' (default) or 'api'")
    var tweetSource: String = "disk"

    @Option(name = "-runtime",
      usage = "print only runtimes, default true")
    var runtimeMeasurements: Boolean = false
  }

  def main(args: Array[String]): Unit = {

    val parser = new CmdLineParser(MainArgs)
    try {
      parser.parseArgument(args.toList.asJava)
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }
    TwitterClustering.execute(MainArgs)
  }
}
