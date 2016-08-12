package de.hpi.isg.mmds.sparkstreaming

import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConverters._

object Main {

  object MainArgs {

    @Option(name = "-input",
      usage = "path to input file")
    var inputPath: String = _

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
      usage = "dimensions for vectorization, default value 100")
    var vectorDimensions: Int = 100

    @Option(name = "-batchDuration",
      usage = "batch duration in seconds, default value 10")
    var batchDuration: Int = 10

    @Option(name = "-source",
      usage = "source for tweets, either 'disk' (default) or 'api'")
    var tweetSource: String = "disk"

    @Option(name = "-runtime",
      usage = "print only runtimes, default true")
    var runtimeMeasurements: Boolean = false

    @Option(name = "-addThreshold",
      usage = "threshold distance for adding a new cluster, default value 2.2")
    var addThreshold: Double = 2.2

    @Option(name = "-mergeThreshold",
      usage = "threshold distance for merging two new cluster, default value 25")
    var mergeThreshold: Double = 25.0
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
    TwitterClustering(MainArgs).execute()
  }
}
