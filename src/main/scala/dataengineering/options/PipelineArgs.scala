package dataengineering.options

import com.spotify.scio.Args

case class PipelineArgs(
  inputSubscription: String,
  outputBQTable: String,
  userGraphPath: Option[String] = None,
  dlqTopic: Option[String] = None,
  sideInputBQTable: Option[String] = None
)

object PipelineArgs {

  def buzzDetectionPipelineArgs(args: Args): PipelineArgs = {
    PipelineArgs(
      inputSubscription = args("inputSubscription"),
      outputBQTable = args("outputBQTable"),
      userGraphPath = Some(args("userGraphPath")),
    )
  }

  def commentToxcityPipelineArgs(args: Args): PipelineArgs = {
    PipelineArgs(
      inputSubscription = args("inputSubscription"),
      outputBQTable = args("outputBQTable"),
      dlqTopic = Some(args("dlqTopic")),
      sideInputBQTable = Some(args("sideInputBQTable"))
    )
  }
}
