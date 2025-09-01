package dataengineering.options

import com.spotify.scio.Args

case class PipelineArgs(inputSubscription: String, outputBQTable: String, userGraphPath: String)

object PipelineArgs {
  def fromArgs(args: Args): PipelineArgs = {
    PipelineArgs(
      inputSubscription = args("inputSubscription"),
      outputBQTable = args("outputBQTable"),
      userGraphPath = args("userGraphPath")
    )
  }
}
